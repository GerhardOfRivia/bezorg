// Package retry classifies transfer failures and computes backoff delays.
package retry

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/textproto"
	"os"
	"strings"
	"syscall"
	"time"
)

// Kind is how a caller should react to an error.
type Kind int

const (
	// KindFatal should not be retried (authentication, missing file, bad config).
	KindFatal Kind = iota
	// KindTransient is a dropped connection, timeout, or short read.
	KindTransient
	// KindLimit means the server refused another connection. Back off and
	// reduce parallelism.
	KindLimit
	// KindRestart means the partial file cannot be resumed and must be
	// truncated before the next attempt.
	KindRestart
)

// ErrRestart is wrapped into errors that require a full re-download.
var ErrRestart = errors.New("transfer must restart from offset 0")

// ErrPermanent marks an error that must not be retried.
var ErrPermanent = errors.New("permanent failure")

// Classify reports how to handle err. A nil error is fatal-not-actionable
// and is reported as KindFatal so callers do not retry success.
func Classify(err error) Kind {
	if err == nil {
		return KindFatal
	}
	if errors.Is(err, ErrRestart) {
		return KindRestart
	}
	if errors.Is(err, ErrPermanent) {
		return KindFatal
	}
	if strings.HasPrefix(err.Error(), "config:") {
		return KindFatal
	}
	if errors.Is(err, context.Canceled) {
		return KindFatal
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return KindTransient
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return KindTransient
	}
	if isLimit(err) {
		return KindLimit
	}
	var tp *textproto.Error
	if errors.As(err, &tp) {
		switch tp.Code {
		case 421:
			return KindLimit
		case 425, 426, 450, 451, 452:
			return KindTransient
		case 500, 501, 502, 503, 530, 532, 533, 534, 550, 551, 552, 553:
			return KindFatal
		default:
			if tp.Code >= 400 && tp.Code < 500 {
				return KindTransient
			}
			if tp.Code >= 500 {
				return KindFatal
			}
		}
	}
	if errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.ENOSPC) {
		return KindFatal
	}
	if isNet(err) {
		return KindTransient
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "too many connections"),
		strings.Contains(msg, "too many users"),
		strings.Contains(msg, "connection limit"):
		return KindLimit
	case strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "use of closed network connection"),
		strings.Contains(msg, "i/o timeout"),
		strings.Contains(msg, "timed out"),
		strings.Contains(msg, "timeout"),
		strings.Contains(msg, "unexpected eof"),
		strings.Contains(msg, "server closed"),
		strings.Contains(msg, "connection closed"),
		strings.Contains(msg, "no route to host"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "handshake failed"),
		strings.Contains(msg, "disconnect"),
		strings.Contains(msg, "stall"):
		return KindTransient
	case strings.Contains(msg, "permission denied"),
		strings.Contains(msg, "authentication failed"),
		strings.Contains(msg, "unable to authenticate"),
		strings.Contains(msg, "login incorrect"),
		strings.Contains(msg, "access denied"),
		strings.Contains(msg, "host key"),
		strings.Contains(msg, "knownhosts"):
		return KindFatal
	default:
		return KindTransient
	}
}

// Transient reports whether the operation should be retried.
func Transient(err error) bool {
	switch Classify(err) {
	case KindTransient, KindLimit, KindRestart:
		return true
	default:
		return false
	}
}

func isLimit(err error) bool {
	var tp *textproto.Error
	if errors.As(err, &tp) && tp.Code == 421 {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "too many connections") ||
		strings.Contains(msg, "too many users") ||
		strings.Contains(msg, "connection limit")
}

func isNet(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	return errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT)
}

// Backoff is exponential from base, capped at max, with full jitter in
// [0.5, 1.0] of the exponential delay. attempt is 1 for the first retry.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for i := 1; i < attempt; i++ {
		if delay > max/2 {
			delay = max
			break
		}
		delay *= 2
	}
	if delay > max {
		delay = max
	}
	span := float64(delay)
	factor := 0.5 + rand.Float64()*0.5
	return time.Duration(span * factor)
}

// Sleep waits for delay or returns ctx.Err when the context ends.
func Sleep(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
