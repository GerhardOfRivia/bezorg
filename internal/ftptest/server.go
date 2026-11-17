// Package ftptest is an in-process FTP server for bezorg tests.
// It speaks enough of RFC 959 and MLSD to exercise listing, REST, RETR,
// passive mode, and NOOP during a transfer.
package ftptest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Server is a passive-only FTP endpoint bound to 127.0.0.1.
type Server struct {
	Addr     string
	User     string
	Password string

	Noops  atomic.Int64
	Logins atomic.Int64

	// FailLogins makes the next N successful USER/PASS exchanges return 421.
	FailLogins atomic.Int32
	// ChunkDelay pauses between data chunks so clients can send NOOP.
	ChunkDelay time.Duration

	ln    net.Listener
	mu    sync.Mutex
	files map[string][]byte
	rests []int64
	conns map[net.Conn]struct{}
	done  chan struct{}
	wg    sync.WaitGroup
}

// Start listens and serves until Close.
func Start() (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &Server{
		Addr:     ln.Addr().String(),
		User:     "user",
		Password: "pass",
		ln:       ln,
		files:    map[string][]byte{},
		conns:    map[net.Conn]struct{}{},
		done:     make(chan struct{}),
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(conn)
			}()
		}
	}()
	return s, nil
}

// Close stops the listener and active sessions.
func (s *Server) Close() error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	err := s.ln.Close()
	s.mu.Lock()
	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

// Put stores a file at an absolute FTP path.
func (s *Server) Put(path string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[clean(path)] = append([]byte(nil), data...)
}

// RESTOffsets returns REST offsets observed by RETR.
func (s *Server) RESTOffsets() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, len(s.rests))
	copy(out, s.rests)
	return out
}

func (s *Server) serve(conn net.Conn) {
	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
	}()
	c := &ctrl{s: s, conn: conn, w: newWriter(conn), cwd: "/"}
	_ = c.write("220 bezorg test ftp ready")
	buf := make([]byte, 0, 256)
	tmp := make([]byte, 1)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, err := conn.Read(tmp)
		if err != nil {
			return
		}
		if tmp[0] == '\n' {
			line := strings.TrimRight(string(buf), "\r")
			buf = buf[:0]
			if err := c.command(line); err != nil {
				return
			}
			continue
		}
		buf = append(buf, tmp[0])
		if len(buf) > 1<<20 {
			return
		}
	}
}

type ctrl struct {
	s      *Server
	conn   net.Conn
	w      *lockedWriter
	cwd    string
	authed bool
	offset int64
	pasv   net.Listener
}

func (c *ctrl) command(line string) error {
	if line == "" {
		return nil
	}
	verb, arg, _ := strings.Cut(line, " ")
	verb = strings.ToUpper(verb)
	arg = strings.TrimSpace(arg)
	switch verb {
	case "USER":
		return c.write("331 password required")
	case "PASS":
		if c.s.FailLogins.Load() > 0 {
			c.s.FailLogins.Add(-1)
			_ = c.write("421 too many connections")
			return io.EOF
		}
		if arg != c.s.Password {
			return c.write("530 login incorrect")
		}
		c.authed = true
		c.s.Logins.Add(1)
		return c.write("230 logged in")
	case "FEAT":
		c.w.mu.Lock()
		_, err := io.WriteString(c.w.w, "211-Features:\r\n MLST type*;size*;modify*;\r\n UTF8\r\n SIZE\r\n HASH SHA-1*;SHA-256*;MD5*\r\n211 End\r\n")
		if err == nil {
			err = c.w.w.Flush()
		}
		c.w.mu.Unlock()
		return err
	case "OPTS":
		return c.write("200 OK")
	case "TYPE":
		return c.write("200 Type set")
	case "SYST":
		return c.write("215 UNIX Type: L8")
	case "NOOP":
		c.s.Noops.Add(1)
		return c.write("200 OK")
	case "PWD":
		return c.write(fmt.Sprintf("257 %q is current", c.cwd))
	case "CWD":
		path := c.resolve(arg)
		if !c.s.isDir(path) {
			return c.write("550 not a directory")
		}
		c.cwd = path
		return c.write("250 directory changed")
	case "SIZE":
		data, ok := c.s.file(c.resolve(arg))
		if !ok {
			return c.write("550 not found")
		}
		return c.write(fmt.Sprintf("213 %d", len(data)))
	case "REST":
		n, err := strconv.ParseInt(arg, 10, 64)
		if err != nil || n < 0 {
			return c.write("501 bad REST")
		}
		c.offset = n
		return c.write("350 restart position accepted")
	case "PASV":
		return c.passive()
	case "EPSV":
		return c.write("502 EPSV not supported")
	case "MLSD", "LIST":
		return c.data(c.listing(c.resolve(arg)))
	case "RETR":
		return c.retr(c.resolve(arg))
	case "HASH":
		return c.hash(c.resolve(arg))
	case "QUIT":
		_ = c.write("221 bye")
		return io.EOF
	default:
		return c.write("502 command not implemented")
	}
}

func (c *ctrl) hash(path string) error {
	data, ok := c.s.file(path)
	if !ok {
		return c.write("550 not found")
	}
	sum := sha256.Sum256(data)
	return c.write("213 SHA-256 " + hex.EncodeToString(sum[:]))
}

func (c *ctrl) retr(path string) error {
	data, ok := c.s.file(path)
	if !ok {
		return c.write("550 not found")
	}
	offset := c.offset
	c.offset = 0
	c.s.mu.Lock()
	c.s.rests = append(c.s.rests, offset)
	c.s.mu.Unlock()
	if offset > int64(len(data)) {
		return c.write("554 invalid REST")
	}
	return c.data(data[offset:])
}

func (c *ctrl) data(payload []byte) error {
	if c.pasv == nil {
		return c.write("425 use PASV first")
	}
	ln := c.pasv
	c.pasv = nil
	if err := c.write("150 opening data connection"); err != nil {
		_ = ln.Close()
		return err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		dc, err := ln.Accept()
		if err != nil {
			return
		}
		defer dc.Close()
		chunk := 1024
		if c.s.ChunkDelay <= 0 {
			chunk = len(payload)
			if chunk == 0 {
				chunk = 1
			}
		}
		for len(payload) > 0 {
			n := chunk
			if n > len(payload) {
				n = len(payload)
			}
			if _, err := dc.Write(payload[:n]); err != nil {
				return
			}
			payload = payload[n:]
			if c.s.ChunkDelay > 0 && len(payload) > 0 {
				time.Sleep(c.s.ChunkDelay)
			}
		}
	}()
	go func() {
		<-done
		_ = c.write("226 transfer complete")
	}()
	return nil
}

func (c *ctrl) passive() error {
	if c.pasv != nil {
		_ = c.pasv.Close()
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return c.write("425 cannot open passive port")
	}
	c.pasv = ln
	tcp := ln.Addr().(*net.TCPAddr)
	p1 := tcp.Port / 256
	p2 := tcp.Port % 256
	return c.write(fmt.Sprintf("227 Entering Passive Mode (127,0,0,1,%d,%d)", p1, p2))
}

func (c *ctrl) listing(dir string) []byte {
	var b strings.Builder
	for _, entry := range c.s.list(dir) {
		kind := "file"
		if entry.dir {
			kind = "dir"
		}
		fmt.Fprintf(&b, "type=%s;size=%d;modify=20261117120000; %s\r\n", kind, entry.size, entry.name)
	}
	return []byte(b.String())
}

func (c *ctrl) resolve(arg string) string {
	if arg == "" {
		return c.cwd
	}
	if strings.HasPrefix(arg, "/") {
		return clean(arg)
	}
	return clean(c.cwd + "/" + arg)
}

func (c *ctrl) write(line string) error {
	return c.w.write(line + "\r\n")
}

type entry struct {
	name string
	size int64
	dir  bool
}

func (s *Server) file(path string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[clean(path)]
	if !ok {
		return nil, false
	}
	return data, true
}

func (s *Server) isDir(path string) bool {
	path = clean(path)
	if path == "/" {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.files[path]; ok {
		return false
	}
	prefix := path + "/"
	for name := range s.files {
		if strings.HasPrefix(name, prefix) || path == "/" {
			return true
		}
	}
	return false
}

func (s *Server) list(dir string) []entry {
	dir = clean(dir)
	prefix := dir
	if prefix != "/" {
		prefix += "/"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dirs := map[string]struct{}{}
	var out []entry
	for name, data := range s.files {
		if dir != "/" && !strings.HasPrefix(name, prefix) {
			continue
		}
		rest := strings.TrimPrefix(name, prefix)
		if dir == "/" {
			rest = strings.TrimPrefix(name, "/")
		}
		if rest == "" || strings.Contains(rest, "/") == false && dir == "/" && strings.Contains(name[1:], "/") {
			// handled below
		}
		if i := strings.Index(rest, "/"); i >= 0 {
			dirs[rest[:i]] = struct{}{}
			continue
		}
		if rest == "" {
			continue
		}
		out = append(out, entry{name: rest, size: int64(len(data))})
	}
	for name := range dirs {
		out = append(out, entry{name: name, dir: true})
	}
	return out
}

func clean(path string) string {
	path = strings.TrimSpace(path)
	path = strings.ReplaceAll(path, "\\", "/")
	for strings.Contains(path, "//") {
		path = strings.ReplaceAll(path, "//", "/")
	}
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if len(path) > 1 {
		path = strings.TrimRight(path, "/")
	}
	return path
}

type lockedWriter struct {
	mu sync.Mutex
	w  interface {
		io.Writer
		Flush() error
	}
}

func newWriter(conn net.Conn) *lockedWriter {
	return &lockedWriter{w: &flushWriter{conn: conn}}
}

func (w *lockedWriter) write(s string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := io.WriteString(w.w, s); err != nil {
		return err
	}
	return w.w.Flush()
}

type flushWriter struct{ conn net.Conn }

func (f *flushWriter) Write(p []byte) (int, error) { return f.conn.Write(p) }
func (f *flushWriter) Flush() error                { return nil }
