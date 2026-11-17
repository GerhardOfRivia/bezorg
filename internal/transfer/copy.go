package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type progressReader struct {
	r    io.Reader
	mu   sync.Mutex
	last time.Time
	read int64
}

func (p *progressReader) Read(buf []byte) (int, error) {
	n, err := p.r.Read(buf)
	if n > 0 {
		p.mu.Lock()
		p.last = time.Now()
		p.read += int64(n)
		p.mu.Unlock()
	}
	return n, err
}

// unblock interrupts a stuck Read without calling Close.
// SFTP holds its file lock across Read, so Close from the watchdog deadlocks.
func unblock(src io.ReadCloser) {
	if a, ok := src.(interface{ Abort() }); ok {
		a.Abort()
		return
	}
	if d, ok := src.(interface{ SetDeadline(time.Time) error }); ok {
		_ = d.SetDeadline(time.Unix(1, 0))
		return
	}
	_ = src.Close()
}

func (p *progressReader) snapshot() (time.Time, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last, p.read
}

// copyStream copies src to dst until EOF. It never buffers the whole object.
// A stall watchdog closes src when no bytes arrive within stall, and lim
// optionally paces the copy. onWrite is called with each chunk length.
func copyStream(ctx context.Context, dst io.Writer, src io.ReadCloser, stall time.Duration, lim *rate.Limiter, bufSize int, onWrite func(int64)) (int64, error) {
	if bufSize < 1 {
		bufSize = 256 * 1024
	}
	reader := &progressReader{r: src, last: time.Now()}
	done := make(chan struct{})
	defer close(done)
	stallErr := make(chan error, 1)
	if stall > 0 {
		tick := stall / 4
		if tick < 20*time.Millisecond {
			tick = 20 * time.Millisecond
		}
		if tick > time.Second {
			tick = time.Second
		}
		go func() {
			ticker := time.NewTicker(tick)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ctx.Done():
					unblock(src)
					return
				case <-ticker.C:
					last, _ := reader.snapshot()
					if time.Since(last) > stall {
						unblock(src)
						select {
						case stallErr <- fmt.Errorf("stall timeout after %s", stall):
						default:
						}
						return
					}
				}
			}
		}()
	}

	buf := make([]byte, bufSize)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n, err := reader.Read(buf)
		if n > 0 {
			if lim != nil {
				if waitErr := lim.WaitN(ctx, n); waitErr != nil {
					return written, waitErr
				}
			}
			wn, werr := dst.Write(buf[:n])
			written += int64(wn)
			if onWrite != nil && wn > 0 {
				onWrite(int64(wn))
			}
			if werr != nil {
				return written, werr
			}
			if wn != n {
				return written, io.ErrShortWrite
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return written, nil
			}
			select {
			case stall := <-stallErr:
				return written, stall
			default:
			}
			if ctx.Err() != nil {
				return written, ctx.Err()
			}
			return written, err
		}
	}
}
