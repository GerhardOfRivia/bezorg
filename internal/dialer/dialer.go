// Package dialer selects FTP, FTPS, or SFTP and opens a session.
package dialer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/proto/ftp"
	"github.com/GerhardOfRivia/bezorg/internal/proto/sftp"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
)

// ResolveProtocol fills cfg.Protocol and cfg.Port. Auto reads the service
// banner: an SSH banner selects SFTP and a 220 banner selects FTP.
// Explicit FTPS is never guessed from a clear-text banner; set protocol to
// ftps or ftps-implicit for TLS servers.
func ResolveProtocol(ctx context.Context, cfg config.Config) (config.Config, error) {
	cfg, err := config.Validate(cfg)
	if err != nil {
		return config.Config{}, err
	}
	if cfg.Protocol != config.ProtocolAuto {
		if cfg.Port == 0 {
			cfg.Port = config.DefaultPort(cfg.Protocol)
		}
		return cfg, nil
	}
	timeout := cfg.ConnectTimeout.Std()
	ports := []int{cfg.Port}
	if cfg.Port == 0 {
		ports = []int{21, 22}
	}
	var last error
	for i, port := range ports {
		protocol, err := probe(ctx, cfg.Host, port, timeout)
		if err == nil {
			cfg.Protocol = protocol
			cfg.Port = port
			return cfg, nil
		}
		last = err
		if i+1 < len(ports) && !refused(err) {
			break
		}
	}
	if last == nil {
		last = errors.New("protocol detection failed")
	}
	return config.Config{}, fmt.Errorf("dialer: detect protocol for %s: %w", cfg.Host, last)
}

// Dial opens a session for the resolved protocol.
func Dial(ctx context.Context, cfg config.Config, logger *slog.Logger) (remote.Session, error) {
	resolved, err := ResolveProtocol(ctx, cfg)
	if err != nil {
		return nil, err
	}
	switch resolved.Protocol {
	case config.ProtocolSFTP:
		return sftp.Dial(ctx, resolved, logger)
	default:
		return ftp.Dial(ctx, resolved, logger)
	}
}

// Checksummer opens a server-side hash helper when the protocol has one.
// SFTP returns a helper that reports checksums as unsupported; sidecar files
// are still read through the data session.
func Checksummer(ctx context.Context, cfg config.Config, logger *slog.Logger) (remote.Checksummer, error) {
	resolved, err := ResolveProtocol(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if resolved.Protocol == config.ProtocolSFTP {
		return noop{}, nil
	}
	return ftp.NewChecksummer(ctx, resolved, logger)
}

type noop struct{}

func (noop) Sum(context.Context, string, string) (string, bool, error) { return "", false, nil }
func (noop) Close() error                                              { return nil }

func probe(ctx context.Context, host string, port int, timeout time.Duration) (string, error) {
	if port == 990 {
		return config.ProtocolFTPSImplicit, nil
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dialer := net.Dialer{Timeout: timeout}
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) && line == "" {
		return "", fmt.Errorf("read banner from %s: %w", addr, err)
	}
	banner := strings.TrimSpace(line)
	switch {
	case strings.HasPrefix(banner, "SSH-"):
		return config.ProtocolSFTP, nil
	case strings.HasPrefix(banner, "220"):
		return config.ProtocolFTP, nil
	default:
		return "", fmt.Errorf("unrecognized banner %q from %s", truncate(banner, 80), addr)
	}
}

func refused(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") || strings.Contains(msg, "no route")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
