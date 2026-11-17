// Package control is the local HTTP API a bezorg client uses to talk to bezorgd.
// The transport is a Unix socket owned by the daemon user.
package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/job"
	"github.com/GerhardOfRivia/bezorg/internal/lock"
)

const (
	maxBody = 1 << 20
)

// ErrAlreadyRunning means another daemon owns the socket.
var ErrAlreadyRunning = errors.New("bezorgd is already running")

// ErrDaemonUnavailable means the socket could not be dialed.
var ErrDaemonUnavailable = errors.New("bezorgd is not running")

// Service is the daemon surface exposed on the socket.
type Service interface {
	Submit(ctx context.Context, cfg config.Config) (job.Record, error)
	List(ctx context.Context) ([]job.Record, error)
	Get(ctx context.Context, selector string) (job.Record, error)
	Cancel(ctx context.Context, selector string) (job.Record, error)
	Retry(ctx context.Context, selector string) (job.Record, error)
	Logs(ctx context.Context, selector string, after int) ([]string, int, error)
	System(ctx context.Context) (System, error)
}

// System describes the running daemon.
type System struct {
	Version   string    `json:"version"`
	Socket    string    `json:"socket"`
	StateDir  string    `json:"state_dir"`
	Active    int       `json:"active"`
	Queued    int       `json:"queued"`
	StartedAt time.Time `json:"started_at"`
}

// Server serves Service on a Unix socket.
type Server struct {
	http     *http.Server
	listener net.Listener
	lock     *os.File
	path     string
}

// Listen acquires the socket. Serve starts accepting.
func Listen(socketPath string, svc Service) (*Server, error) {
	if svc == nil {
		return nil, errors.New("control: service is required")
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return nil, err
	}
	lockFile, err := lock.Exclusive(socketPath + ".lock")
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrAlreadyRunning, err)
	}
	if err := prepareSocket(socketPath); err != nil {
		_ = lockFile.Close()
		return nil, err
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		_ = lockFile.Close()
		return nil, err
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(socketPath)
		_ = lockFile.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	server := &Server{listener: listener, lock: lockFile, path: socketPath}
	mux.HandleFunc("GET /v1/system", func(w http.ResponseWriter, r *http.Request) {
		system, err := svc.System(r.Context())
		write(w, system, err)
	})
	mux.HandleFunc("POST /v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		cfg := config.Defaults()
		if err := decode(w, r, &cfg); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		rec, err := svc.Submit(r.Context(), cfg)
		write(w, map[string]any{"job": rec}, err)
	})
	mux.HandleFunc("GET /v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		jobs, err := svc.List(r.Context())
		write(w, map[string]any{"jobs": jobs}, err)
	})
	mux.HandleFunc("GET /v1/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		rec, err := svc.Get(r.Context(), r.PathValue("id"))
		write(w, map[string]any{"job": rec}, err)
	})
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		rec, err := svc.Cancel(r.Context(), r.PathValue("id"))
		write(w, map[string]any{"job": rec}, err)
	})
	mux.HandleFunc("POST /v1/jobs/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
		rec, err := svc.Retry(r.Context(), r.PathValue("id"))
		write(w, map[string]any{"job": rec}, err)
	})
	mux.HandleFunc("GET /v1/jobs/{id}/logs", func(w http.ResponseWriter, r *http.Request) {
		after, _ := strconv.Atoi(r.URL.Query().Get("after"))
		lines, next, err := svc.Logs(r.Context(), r.PathValue("id"), after)
		write(w, map[string]any{"lines": lines, "next": next}, err)
	})
	server.http = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return server, nil
}

// Serve accepts until Shutdown or Close.
func (s *Server) Serve() error {
	err := s.http.Serve(s.listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown stops accepting and waits for active requests.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.http == nil {
		return nil
	}
	err := s.http.Shutdown(ctx)
	if s.lock != nil {
		_ = s.lock.Close()
		s.lock = nil
	}
	if s.path != "" {
		_ = os.Remove(s.path)
	}
	return err
}

// Path is the socket filesystem path.
func (s *Server) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func prepareSocket(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("control: %s exists and is not a socket", path)
	}
	conn, dialErr := net.DialTimeout("unix", path, 200*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		return fmt.Errorf("%w at %s", ErrAlreadyRunning, path)
	}
	return os.Remove(path)
}

func decode(w http.ResponseWriter, r *http.Request, dest any) error {
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		return err
	}
	if len(data) > maxBody {
		return errors.New("request body is too large")
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("invalid json: %w", err)
	}
	return nil
}

func write(w http.ResponseWriter, body any, err error) {
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, job.ErrNotFound):
			status = http.StatusNotFound
		case errors.Is(err, job.ErrAmbiguous):
			status = http.StatusConflict
		case strings.HasPrefix(err.Error(), "config:"):
			status = http.StatusBadRequest
		}
		writeErr(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, body)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(body)
}

// Client talks to a running bezorgd.
type Client struct {
	socket string
	http   *http.Client
}

// NewClient dials socketPath for each request.
func NewClient(socketPath string) *Client {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	transport := &http.Transport{
		DisableCompression: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, "unix", socketPath)
			if err != nil {
				return nil, fmt.Errorf("%w at %s: %v", ErrDaemonUnavailable, socketPath, err)
			}
			return conn, nil
		},
	}
	return &Client{
		socket: socketPath,
		http:   &http.Client{Transport: transport, Timeout: 30 * time.Second},
	}
}

// Socket is the path this client dials.
func (c *Client) Socket() string { return c.socket }

// Submit queues a job.
func (c *Client) Submit(ctx context.Context, cfg config.Config) (job.Record, error) {
	var response struct {
		Job job.Record `json:"job"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/jobs", cfg, &response)
	return response.Job, err
}

// List returns every retained job.
func (c *Client) List(ctx context.Context) ([]job.Record, error) {
	var response struct {
		Jobs []job.Record `json:"jobs"`
	}
	err := c.do(ctx, http.MethodGet, "/v1/jobs", nil, &response)
	return response.Jobs, err
}

// Get fetches one job by id or unique prefix.
func (c *Client) Get(ctx context.Context, selector string) (job.Record, error) {
	var response struct {
		Job job.Record `json:"job"`
	}
	err := c.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(selector), nil, &response)
	return response.Job, err
}

// Cancel asks the daemon to stop a job.
func (c *Client) Cancel(ctx context.Context, selector string) (job.Record, error) {
	var response struct {
		Job job.Record `json:"job"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(selector)+"/cancel", map[string]any{}, &response)
	return response.Job, err
}

// Retry queues a finished job again. Completed files are skipped.
func (c *Client) Retry(ctx context.Context, selector string) (job.Record, error) {
	var response struct {
		Job job.Record `json:"job"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(selector)+"/retry", map[string]any{}, &response)
	return response.Job, err
}

// Logs returns a page of log lines.
func (c *Client) Logs(ctx context.Context, selector string, after int) ([]string, int, error) {
	var response struct {
		Lines []string `json:"lines"`
		Next  int      `json:"next"`
	}
	path := "/v1/jobs/" + url.PathEscape(selector) + "/logs?after=" + strconv.Itoa(after)
	err := c.do(ctx, http.MethodGet, path, nil, &response)
	return response.Lines, response.Next, err
}

// System returns daemon metadata.
func (c *Client) System(ctx context.Context) (System, error) {
	var system System
	err := c.do(ctx, http.MethodGet, "/v1/system", nil, &system)
	return system, err
}

func (c *Client) do(ctx context.Context, method, path string, body, dest any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://bezorg"+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		var payload struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &payload)
		if payload.Error == "" {
			payload.Error = resp.Status
		}
		return fmt.Errorf("bezorgd: %s", payload.Error)
	}
	if dest == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("bezorgd response: %w", err)
	}
	return nil
}
