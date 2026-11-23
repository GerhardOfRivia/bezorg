// Package webui serves a small dashboard compiled into bezorgd.
package webui

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/control"
)

const shutdownTimeout = 5 * time.Second

//go:embed dashboard.html
var dashboardHTML []byte

//go:embed dashboard.css
var dashboardCSS []byte

//go:embed dashboard.js
var dashboardJS []byte

// Server is the optional HTTP dashboard. It listens on loopback, or on a
// wildcard address when that is requested explicitly. Other hosts are refused.
type Server struct {
	listener   net.Listener
	httpServer *http.Server
	address    string
	token      string
	tokenPath  string

	closeOnce sync.Once
	closeErr  error
}

// NewServer binds address and writes a bearer token to tokenPath. The token
// directory must be private (no group or world permissions). publicRead allows
// anonymous GET requests; cancel and retry always require the token.
func NewServer(address, tokenPath string, svc control.Service, logger *slog.Logger, publicRead bool) (*Server, error) {
	if svc == nil {
		return nil, errors.New("webui: service is required")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, errors.New("webui: listen address is required")
	}
	wildcard, err := validateListenAddress(address)
	if err != nil {
		return nil, err
	}
	tokenPath = strings.TrimSpace(tokenPath)
	if tokenPath == "" {
		return nil, errors.New("webui: token path is required")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("webui: listen on %s: %w", address, err)
	}
	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok || tcpAddress.IP == nil || (!tcpAddress.IP.IsLoopback() && !(wildcard && tcpAddress.IP.IsUnspecified())) {
		_ = listener.Close()
		return nil, fmt.Errorf("webui: resolved listen address %q is neither loopback nor wildcard", address)
	}
	token, err := writeAccessToken(tokenPath)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	handler := newHandler(svc, token, wildcard, publicRead)
	server := &Server{
		listener:  listener,
		address:   listener.Addr().String(),
		token:     token,
		tokenPath: tokenPath,
	}
	server.httpServer = &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		// Long-poll and SSE may hold the response open for up to 60s.
		WriteTimeout:   90 * time.Second,
		IdleTimeout:    time.Minute,
		MaxHeaderBytes: 16 << 10,
	}
	if wildcard {
		logger.Warn("web dashboard is listening on all network interfaces; HTTP is unencrypted, so protect the access token and restrict network access",
			"address", server.address)
	}
	return server, nil
}

func writeAccessToken(filename string) (string, error) {
	directory := filepath.Dir(filename)
	info, err := os.Stat(directory)
	if err != nil {
		return "", fmt.Errorf("webui: inspect token directory %s: %w", directory, err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("webui: token directory %s must be a private directory", directory)
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("webui: generate access token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(buf)
	temporary, err := os.CreateTemp(directory, ".bezorg-web-token-*")
	if err != nil {
		return "", fmt.Errorf("webui: create temporary token: %w", err)
	}
	temporaryName := temporary.Name()
	cleanup := true
	defer func() {
		_ = temporary.Close()
		if cleanup {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return "", fmt.Errorf("webui: protect temporary token: %w", err)
	}
	if _, err := temporary.WriteString(token + "\n"); err != nil {
		return "", fmt.Errorf("webui: write temporary token: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return "", fmt.Errorf("webui: sync temporary token: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("webui: close temporary token: %w", err)
	}
	if err := os.Rename(temporaryName, filename); err != nil {
		return "", fmt.Errorf("webui: install token %s: %w", filename, err)
	}
	cleanup = false
	return token, nil
}

func removeAccessToken(filename, token string) {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return
	}
	current := strings.TrimSpace(string(contents))
	if subtle.ConstantTimeCompare([]byte(current), []byte(token)) == 1 {
		_ = os.Remove(filename)
	}
}

func validateListenAddress(address string) (bool, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false, fmt.Errorf("webui: listen address must be host:port: %w", err)
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return false, nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false, fmt.Errorf("webui: refusing non-loopback listen address %q; use an authenticated tunnel for remote access", address)
	}
	if ip.IsLoopback() {
		return false, nil
	}
	if ip.IsUnspecified() {
		return true, nil
	}
	return false, fmt.Errorf("webui: refusing non-loopback listen address %q; use a wildcard listener or authenticated tunnel for remote access", address)
}

// Address returns the bound TCP address.
func (server *Server) Address() string {
	if server == nil {
		return ""
	}
	return server.address
}

// TokenPath returns the file holding the bearer token.
func (server *Server) TokenPath() string {
	if server == nil {
		return ""
	}
	return server.tokenPath
}

// Serve handles requests until ctx is canceled, Close is called, or the
// listener fails.
func (server *Server) Serve(ctx context.Context) error {
	if server == nil || server.listener == nil || server.httpServer == nil {
		return errors.New("webui: server is required")
	}
	if ctx == nil {
		return errors.New("webui: serve context is required")
	}

	done := make(chan error, 1)
	go func() {
		done <- server.httpServer.Serve(server.listener)
	}()

	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := server.httpServer.Shutdown(shutdownContext)
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, server.httpServer.Close())
	}
	serveErr := <-done
	if errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, net.ErrClosed) {
		serveErr = nil
	}
	return errors.Join(shutdownErr, serveErr)
}

// Close releases the listener and deletes the token file when it still holds
// this process's token. It is safe to call before Serve and more than once.
func (server *Server) Close() error {
	if server == nil {
		return nil
	}
	server.closeOnce.Do(func() {
		var errs []error
		if server.httpServer != nil {
			if err := server.httpServer.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		if server.listener != nil {
			if err := server.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		server.closeErr = errors.Join(errs...)
		removeAccessToken(server.tokenPath, server.token)
	})
	return server.closeErr
}

func newHandler(svc control.Service, token string, allowRemoteIPHosts, publicRead bool) http.Handler {
	api := apiServer{svc: svc, publicRead: publicRead}
	mux := http.NewServeMux()
	publicReads := make(map[string]bool)
	for pattern, handler := range api.routes() {
		mux.HandleFunc(pattern, handler)
		if publicRead && strings.HasPrefix(pattern, "GET ") {
			publicReads[pattern] = true
		}
	}
	mux.HandleFunc("/api/", func(output http.ResponseWriter, request *http.Request) {
		writeAPIError(output, http.StatusNotFound, "API endpoint not found")
	})
	mux.HandleFunc("GET /{$}", serveAsset(dashboardHTML, "text/html; charset=utf-8"))
	mux.HandleFunc("GET /dashboard.css", serveAsset(dashboardCSS, "text/css; charset=utf-8"))
	mux.HandleFunc("GET /dashboard.js", serveAsset(dashboardJS, "text/javascript; charset=utf-8"))

	return securityHeaders(validateHost(requireAPIAuth(token, mux, publicReads), allowRemoteIPHosts))
}

func serveAsset(body []byte, contentType string) http.HandlerFunc {
	return func(output http.ResponseWriter, request *http.Request) {
		output.Header().Set("Content-Type", contentType)
		output.Header().Set("Cache-Control", "no-cache")
		output.Header().Set("X-Content-Type-Options", "nosniff")
		if request.Method != http.MethodHead {
			_, _ = output.Write(body)
		}
	}
}

func requireAPIAuth(token string, next *http.ServeMux, publicReads map[string]bool) http.Handler {
	return http.HandlerFunc(func(output http.ResponseWriter, request *http.Request) {
		if isAPIPath(request.URL.Path) {
			headers := request.Header.Values("Authorization")
			provided, bearer := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer ")
			authenticated := len(headers) == 1 && bearer && token != "" &&
				subtle.ConstantTimeCompare([]byte(provided), []byte(token)) == 1
			// Match the registered method and path, so only explicit GET
			// routes can skip authentication. A bad token is never anonymous.
			_, pattern := next.Handler(request)
			anonymousRead := len(headers) == 0 && publicReads[pattern]
			if !authenticated && !anonymousRead {
				output.Header().Set("WWW-Authenticate", `Bearer realm="bezorg"`)
				writeAPIError(output, http.StatusUnauthorized, "A valid bezorg web token is required")
				return
			}
		}
		next.ServeHTTP(output, request)
	})
}

func isAPIPath(requestPath string) bool {
	return requestPath == "/api" || strings.HasPrefix(requestPath, "/api/")
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(output http.ResponseWriter, request *http.Request) {
		output.Header().Set("Content-Security-Policy", "default-src 'self'; base-uri 'none'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; img-src 'none'; object-src 'none'; script-src 'self'; style-src 'self'")
		output.Header().Set("Referrer-Policy", "no-referrer")
		output.Header().Set("X-Content-Type-Options", "nosniff")
		output.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(output, request)
	})
}

func validateHost(next http.Handler, allowRemoteIPs bool) http.Handler {
	return http.HandlerFunc(func(output http.ResponseWriter, request *http.Request) {
		host := request.Host
		if parsed, _, err := net.SplitHostPort(host); err == nil {
			host = parsed
		}
		host = strings.Trim(host, "[]")
		ip := net.ParseIP(host)
		if !strings.EqualFold(host, "localhost") && (ip == nil || (!ip.IsLoopback() && !(allowRemoteIPs && ip != nil))) {
			http.Error(output, "unrecognized host", http.StatusMisdirectedRequest)
			return
		}
		next.ServeHTTP(output, request)
	})
}
