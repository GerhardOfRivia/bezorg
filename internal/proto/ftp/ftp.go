// Package ftp downloads files over FTP and explicit or implicit FTPS.
// Data is streamed from the socket. Binary mode and REST are used so a
// partial local file can resume after a dropped connection.
package ftp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/textproto"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
	ftplib "github.com/jlaffaye/ftp"
)

// Session is one FTP(S) control connection and its data transfers.
// It is not safe for concurrent transfers. Keep-alive NOOPs run only while
// a large transfer is in progress, and only between control commands.
type Session struct {
	cfg     config.Config
	conn    *ftplib.ServerConn
	control *trackedConn
	log     *slog.Logger

	mu       sync.Mutex
	dirty    bool
	closed   bool
	nextSize int64
	lastCmd  time.Time
	keepStop chan struct{}
	keepDone chan struct{}
	keeping  bool
}

// Dial logs in and switches the session to binary mode.
func Dial(ctx context.Context, cfg config.Config, logger *slog.Logger) (*Session, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	protocol := cfg.Protocol
	if protocol == "" || protocol == config.ProtocolAuto {
		protocol = config.ProtocolFTP
	}
	switch protocol {
	case config.ProtocolFTP, config.ProtocolFTPS, config.ProtocolFTPSImplicit:
	default:
		return nil, fmt.Errorf("ftp: protocol %q is not an FTP mode", protocol)
	}
	if cfg.Port == 0 {
		cfg.Port = config.DefaultPort(protocol)
	}
	addr := cfg.Address(protocol)
	tlsConfig := &tls.Config{
		ServerName:         cfg.Host,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.Insecure,
		ClientSessionCache: tls.NewLRUClientSessionCache(8),
	}
	var control *trackedConn
	var first sync.Once
	dialer := net.Dialer{
		Timeout:   cfg.ConnectTimeout.Std(),
		KeepAlive: 30 * time.Second,
	}
	useTLS := protocol == config.ProtocolFTPS || protocol == config.ProtocolFTPSImplicit
	implicit := protocol == config.ProtocolFTPSImplicit
	dialFunc := func(network, address string) (net.Conn, error) {
		if cfg.ForceIPv4 {
			network = "tcp4"
		}
		raw, err := dialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		isControl := false
		first.Do(func() { isControl = true })
		conn := raw
		if implicit || (useTLS && !isControl) {
			tlsConn := tls.Client(raw, tlsConfig)
			if isControl {
				if err := tlsConn.HandshakeContext(ctx); err != nil {
					_ = raw.Close()
					return nil, err
				}
			}
			conn = tlsConn
		}
		wrapped := &trackedConn{Conn: conn}
		if isControl {
			control = wrapped
		}
		if tcp, ok := raw.(*net.TCPConn); ok {
			_ = tcp.SetKeepAlive(true)
			_ = tcp.SetKeepAlivePeriod(30 * time.Second)
		}
		return wrapped, nil
	}

	opts := []ftplib.DialOption{
		ftplib.DialWithContext(ctx),
		ftplib.DialWithTimeout(cfg.ConnectTimeout.Std()),
		ftplib.DialWithDialer(dialer),
		ftplib.DialWithDialFunc(dialFunc),
		ftplib.DialWithDisabledEPSV(cfg.DisableEPSV),
		ftplib.DialWithDisabledMLSD(cfg.DisableMLSD),
		ftplib.DialWithShutTimeout(cfg.CommandTimeout.Std()),
		ftplib.DialWithTrustPasvIP(cfg.TrustPASVIP),
	}
	if protocol == config.ProtocolFTPS {
		opts = append(opts, ftplib.DialWithExplicitTLS(tlsConfig))
	}
	if implicit {
		opts = append(opts, ftplib.DialWithTLS(tlsConfig))
	}
	conn, err := ftplib.Dial(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("ftp: dial %s: %w", addr, err)
	}
	session := &Session{
		cfg:     cfg,
		conn:    conn,
		control: control,
		log:     logger,
		lastCmd: time.Now(),
	}
	if err := session.withControlDeadline(cfg.CommandTimeout.Std(), func() error {
		return conn.Login(cfg.User, cfg.Password)
	}); err != nil {
		_ = conn.Quit()
		return nil, fmt.Errorf("ftp: login: %w", err)
	}
	if err := conn.Type(ftplib.TransferTypeBinary); err != nil {
		_ = conn.Quit()
		return nil, fmt.Errorf("ftp: binary mode: %w", err)
	}
	cfg.Protocol = protocol
	session.cfg = cfg
	logger.Debug("ftp session ready", "addr", addr, "protocol", protocol)
	return session, nil
}

// Protocol reports the FTP mode used by this session.
func (s *Session) Protocol() string {
	if s == nil {
		return ""
	}
	return s.cfg.Protocol
}

// SetTransferSize tells the session whether the next OpenAt should keep the
// control connection awake. Large imagery transfers need NOOP; small files
// finish before a typical idle timeout and keep the connection reusable.
func (s *Session) SetTransferSize(size int64) {
	s.mu.Lock()
	s.nextSize = size
	s.mu.Unlock()
}

// Unusable reports that the control connection must be redialed.
func (s *Session) Unusable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirty || s.closed || s.conn == nil
}

// List walks remote. A path that is a file yields a one-element listing.
func (s *Session) List(ctx context.Context, remotePath string) (remote.Listing, error) {
	remotePath = cleanRemote(remotePath)
	isFile, err := s.classify(ctx, remotePath)
	if err != nil {
		return remote.Listing{}, err
	}
	if isFile {
		size, err := s.sizeOf(ctx, remotePath)
		if err != nil {
			return remote.Listing{}, err
		}
		return remote.Listing{
			RootIsFile: true,
			Files: []remote.File{{
				Rel:    baseName(remotePath),
				Remote: remotePath,
				Size:   size,
			}},
		}, nil
	}
	var files []remote.File
	seen := map[string]struct{}{}
	if err := s.walk(ctx, remotePath, "", &files, seen); err != nil {
		return remote.Listing{}, err
	}
	return remote.Listing{Files: files}, nil
}

func (s *Session) walk(ctx context.Context, dir, prefix string, files *[]remote.File, seen map[string]struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := cleanRemote(dir)
	if _, ok := seen[key]; ok {
		return nil
	}
	seen[key] = struct{}{}
	entries, err := s.listDir(ctx, dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name
		if name == "" || name == "." || name == ".." {
			continue
		}
		rel := name
		if prefix != "" {
			rel = prefix + "/" + name
		}
		remotePath := joinRemote(dir, name)
		switch entry.Type {
		case ftplib.EntryTypeFolder:
			if err := s.walk(ctx, remotePath, rel, files, seen); err != nil {
				return err
			}
		case ftplib.EntryTypeLink:
			s.log.Warn("skipping symlink", "path", remotePath)
		default:
			size := int64(entry.Size)
			if size == 0 && s.cfg.ProbeZeroSize {
				if probed, probeErr := s.sizeOf(ctx, remotePath); probeErr == nil {
					size = probed
				}
			}
			*files = append(*files, remote.File{Rel: rel, Remote: remotePath, Size: size})
		}
	}
	return nil
}

func (s *Session) listDir(ctx context.Context, dir string) ([]*ftplib.Entry, error) {
	var entries []*ftplib.Entry
	err := s.call(ctx, func() error {
		var err error
		entries, err = s.conn.List(dir)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("ftp: list %s: %w", dir, err)
	}
	s.touch()
	return entries, nil
}

func (s *Session) classify(ctx context.Context, remotePath string) (bool, error) {
	var isFile bool
	err := s.call(ctx, func() error {
		pwd, err := s.conn.CurrentDir()
		if err != nil {
			return err
		}
		if err := s.conn.ChangeDir(remotePath); err == nil {
			if back := s.conn.ChangeDir(pwd); back != nil {
				return back
			}
			isFile = false
			return nil
		}
		if _, err := s.conn.FileSize(remotePath); err == nil {
			isFile = true
			return nil
		}
		entries, err := s.conn.List(remotePath)
		if err != nil {
			return err
		}
		if len(entries) == 1 && entries[0].Type == ftplib.EntryTypeFile {
			base := baseName(remotePath)
			if entries[0].Name == base || entries[0].Name == remotePath {
				isFile = true
				return nil
			}
		}
		isFile = false
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("ftp: stat %s: %w", remotePath, err)
	}
	s.touch()
	return isFile, nil
}

func (s *Session) sizeOf(ctx context.Context, remotePath string) (int64, error) {
	var size int64
	err := s.call(ctx, func() error {
		got, err := s.conn.FileSize(remotePath)
		size = got
		return err
	})
	if err != nil {
		return -1, err
	}
	s.touch()
	return size, nil
}

// OpenAt streams a file starting at offset. offset zero reads from the start.
func (s *Session) OpenAt(ctx context.Context, remotePath string, offset int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("ftp: negative offset %d", offset)
	}
	if err := s.nudge(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	size := s.nextSize
	if s.dirty || s.closed || s.conn == nil {
		s.mu.Unlock()
		return nil, errors.New("ftp: control connection is unusable")
	}
	resp, err := s.conn.RetrFrom(remotePath, uint64(offset))
	if err != nil {
		s.dirty = true
		s.mu.Unlock()
		if isNotFound(err) {
			return nil, fmt.Errorf("ftp: open %s: %w", remotePath, os.ErrNotExist)
		}
		return nil, fmt.Errorf("ftp: retr %s at %d: %w", remotePath, offset, err)
	}
	s.mu.Unlock()
	s.touch()
	stream := &dataStream{resp: resp, session: s}
	if wantKeepalive(size, s.cfg) {
		s.startKeepalive()
	}
	return stream, nil
}

// ReadSmall reads a short object such as a checksum sidecar.
func (s *Session) ReadSmall(ctx context.Context, remotePath string, limit int64) ([]byte, error) {
	if limit < 1 {
		limit = 4096
	}
	s.SetTransferSize(limit)
	body, err := s.OpenAt(ctx, remotePath, 0)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	buf, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) > limit {
		return nil, fmt.Errorf("ftp: %s exceeds %d bytes", remotePath, limit)
	}
	return buf, nil
}

// Close logs out, or drops a desynchronised control connection.
func (s *Session) Close() error {
	s.stopKeepalive()
	s.mu.Lock()
	defer s.mu.Unlock()
	conn := s.conn
	dirty := s.dirty
	s.conn = nil
	s.closed = true
	if conn == nil {
		return nil
	}
	if dirty {
		s.expireControl(2 * time.Second)
		_ = conn.Quit()
		return nil
	}
	return conn.Quit()
}

func (s *Session) nudge() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dirty || s.closed || s.conn == nil {
		return errors.New("ftp: control connection is unusable")
	}
	interval := s.cfg.Keepalive.Std()
	if interval <= 0 || time.Since(s.lastCmd) < interval/2 {
		return nil
	}
	if err := s.conn.NoOp(); err != nil {
		s.dirty = true
		return fmt.Errorf("ftp: noop: %w", err)
	}
	s.lastCmd = time.Now()
	return nil
}

func (s *Session) startKeepalive() {
	interval := s.cfg.Keepalive.Std()
	if interval <= 0 {
		return
	}
	s.mu.Lock()
	if s.keeping || s.closed {
		s.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	s.keepStop = stop
	s.keepDone = done
	s.keeping = true
	s.mu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if !s.noopDuringTransfer() {
					return
				}
			}
		}
	}()
}

func (s *Session) noopDuringTransfer() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.dirty || s.conn == nil {
		return false
	}
	if s.control != nil {
		s.control.setReadLimit(5 * time.Second)
	}
	err := s.conn.NoOp()
	if s.control != nil {
		s.control.setReadLimit(0)
	}
	if err != nil {
		s.dirty = true
		s.log.Debug("keepalive during transfer failed; connection will be redialed", "error", err)
		return false
	}
	s.lastCmd = time.Now()
	return true
}

func (s *Session) stopKeepalive() {
	s.mu.Lock()
	if !s.keeping {
		s.mu.Unlock()
		return
	}
	stop := s.keepStop
	done := s.keepDone
	s.keeping = false
	s.mu.Unlock()
	close(stop)
	select {
	case <-done:
	case <-time.After(6 * time.Second):
	}
}

func (s *Session) call(ctx context.Context, fn func() error) error {
	timeout := s.cfg.CommandTimeout.Std()
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		errc <- fn()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		s.abort()
		<-errc
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("command timed out after %s: %w", timeout, ctx.Err())
		}
		return ctx.Err()
	}
}

func (s *Session) abort() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirty = true
	s.expireControl(time.Second)
}

func (s *Session) expireControl(after time.Duration) {
	if s.control == nil {
		return
	}
	_ = s.control.SetDeadline(time.Now().Add(after))
}

func (s *Session) withControlDeadline(after time.Duration, fn func() error) error {
	if s.control != nil && after > 0 {
		_ = s.control.SetDeadline(time.Now().Add(after))
		defer s.control.SetDeadline(time.Time{})
	}
	return fn()
}

func (s *Session) touch() {
	s.mu.Lock()
	s.lastCmd = time.Now()
	s.mu.Unlock()
}

type dataStream struct {
	resp    *ftplib.Response
	session *Session
	once    sync.Once
	eof     bool
	readErr error
}

func (d *dataStream) Read(p []byte) (int, error) {
	n, err := d.resp.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		d.readErr = err
	}
	if errors.Is(err, io.EOF) {
		d.eof = true
	}
	return n, err
}

func (d *dataStream) SetDeadline(t time.Time) error {
	return d.resp.SetDeadline(t)
}

// Abort unblocks a stalled data read. The following Close still collects the
// control-connection reply.
func (d *dataStream) Abort() {
	_ = d.resp.SetDeadline(time.Unix(1, 0))
}

func (d *dataStream) Close() error {
	var err error
	d.once.Do(func() {
		d.session.stopKeepalive()
		d.session.mu.Lock()
		dirty := d.session.dirty
		d.session.mu.Unlock()
		if dirty {
			_ = d.resp.SetDeadline(time.Now().Add(2 * time.Second))
		}
		err = d.resp.Close()
		if err != nil {
			d.session.mu.Lock()
			d.session.dirty = true
			d.session.mu.Unlock()
			// A fully read object is still usable when the server was slow to
			// send 226, or a NOOP reply raced the completion line. The engine
			// checks the byte count before renaming.
			if d.eof && d.readErr == nil {
				err = nil
			}
		} else {
			d.session.touch()
		}
	})
	return err
}

func wantKeepalive(size int64, cfg config.Config) bool {
	if cfg.Keepalive.Std() <= 0 {
		return false
	}
	floor := cfg.KeepaliveMinBytes
	if floor <= 0 {
		floor = 32 << 20
	}
	if size < 0 {
		return true
	}
	return size >= floor
}

type trackedConn struct {
	net.Conn
	mu        sync.Mutex
	readLimit time.Duration
}

func (c *trackedConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	limit := c.readLimit
	c.mu.Unlock()
	if limit > 0 {
		_ = c.Conn.SetReadDeadline(time.Now().Add(limit))
		defer c.Conn.SetReadDeadline(time.Time{})
	}
	return c.Conn.Read(p)
}

func (c *trackedConn) setReadLimit(limit time.Duration) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.readLimit = limit
	c.mu.Unlock()
}

func cleanRemote(path string) string {
	path = strings.TrimSpace(path)
	path = strings.ReplaceAll(path, "\\", "/")
	if path == "" {
		return "/"
	}
	return path
}

func joinRemote(dir, name string) string {
	dir = strings.TrimRight(cleanRemote(dir), "/")
	if dir == "" {
		return "/" + name
	}
	return dir + "/" + name
}

func baseName(path string) string {
	path = strings.TrimRight(cleanRemote(path), "/")
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[i+1:]
	}
	return path
}

func isNotFound(err error) bool {
	var tp *textproto.Error
	if errors.As(err, &tp) && tp.Code == 550 {
		return true
	}
	return false
}
