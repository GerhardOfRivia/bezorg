// Package sftp downloads files over SSH File Transfer Protocol.
// Reads are streamed and resumed from the local partial file's length.
package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// Client is one SSH connection with an SFTP subsystem.
// It is not safe for concurrent use by multiple transfers.
type Client struct {
	cfg  config.Config
	ssh  *ssh.Client
	sftp *sftp.Client
	log  *slog.Logger

	mu     sync.Mutex
	dead   bool
	closed bool
	done   chan struct{}
}

// Dial authenticates and starts the SFTP subsystem.
func Dial(ctx context.Context, cfg config.Config, logger *slog.Logger) (*Client, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.Port == 0 {
		cfg.Port = config.DefaultPort(config.ProtocolSFTP)
	}
	callback, err := hostCallback(cfg)
	if err != nil {
		return nil, err
	}
	auth, err := authMethods(cfg)
	if err != nil {
		return nil, err
	}
	sshConfig := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            auth,
		HostKeyCallback: callback,
		Timeout:         cfg.ConnectTimeout.Std(),
	}
	dialer := net.Dialer{Timeout: cfg.ConnectTimeout.Std(), KeepAlive: 30 * time.Second}
	network := "tcp"
	if cfg.ForceIPv4 {
		network = "tcp4"
	}
	addr := cfg.Address(config.ProtocolSFTP)
	raw, err := dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("sftp: dial %s: %w", addr, err)
	}
	_ = raw.SetDeadline(time.Now().Add(cfg.ConnectTimeout.Std()))
	sshConn, chans, reqs, err := ssh.NewClientConn(raw, addr, sshConfig)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("sftp: handshake %s: %w", addr, err)
	}
	_ = raw.SetDeadline(time.Time{})
	client := ssh.NewClient(sshConn, chans, reqs)
	session, err := sftp.NewClient(client, sftp.UseConcurrentReads(false), sftp.UseConcurrentWrites(false))
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("sftp: subsystem: %w", err)
	}
	out := &Client{cfg: cfg, ssh: client, sftp: session, log: logger, done: make(chan struct{})}
	go out.keepalive()
	logger.Debug("sftp session ready", "addr", addr)
	return out, nil
}

// Unusable reports that the SSH connection can no longer be reused.
func (c *Client) Unusable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dead || c.closed || c.ssh == nil
}

// List walks remote. A file path yields a single-element listing.
func (c *Client) List(ctx context.Context, remotePath string) (remote.Listing, error) {
	if err := ctx.Err(); err != nil {
		return remote.Listing{}, err
	}
	remotePath = clean(remotePath)
	info, err := c.sftp.Stat(remotePath)
	if err != nil {
		return remote.Listing{}, fmt.Errorf("sftp: stat %s: %w", remotePath, err)
	}
	if !info.IsDir() {
		return remote.Listing{
			RootIsFile: true,
			Files: []remote.File{{
				Rel:    path.Base(remotePath),
				Remote: remotePath,
				Size:   info.Size(),
			}},
		}, nil
	}
	var files []remote.File
	seen := map[string]struct{}{}
	if err := c.walk(ctx, remotePath, "", &files, seen); err != nil {
		return remote.Listing{}, err
	}
	return remote.Listing{Files: files}, nil
}

func (c *Client) walk(ctx context.Context, dir, prefix string, files *[]remote.File, seen map[string]struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir = clean(dir)
	if _, ok := seen[dir]; ok {
		return nil
	}
	seen[dir] = struct{}{}
	entries, err := c.sftp.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("sftp: list %s: %w", dir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "" || name == "." || name == ".." {
			continue
		}
		rel := name
		if prefix != "" {
			rel = prefix + "/" + name
		}
		remotePath := join(dir, name)
		if entry.Mode()&os.ModeSymlink != 0 {
			c.log.Warn("skipping symlink", "path", remotePath)
			continue
		}
		if entry.IsDir() {
			if err := c.walk(ctx, remotePath, rel, files, seen); err != nil {
				return err
			}
			continue
		}
		*files = append(*files, remote.File{Rel: rel, Remote: remotePath, Size: entry.Size()})
	}
	return nil
}

// OpenAt streams remotePath from offset.
func (c *Client) OpenAt(ctx context.Context, remotePath string, offset int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("sftp: negative offset %d", offset)
	}
	file, err := c.sftp.Open(remotePath)
	if err != nil {
		return nil, fmt.Errorf("sftp: open %s: %w", remotePath, err)
	}
	if offset > 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("sftp: seek %s to %d: %w", remotePath, offset, err)
		}
	}
	return &body{file: file, client: c}, nil
}

// ReadSmall reads a short sidecar such as a checksum file.
func (c *Client) ReadSmall(ctx context.Context, remotePath string, limit int64) ([]byte, error) {
	if limit < 1 {
		limit = 4096
	}
	body, err := c.OpenAt(ctx, remotePath, 0)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	buf, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) > limit {
		return nil, fmt.Errorf("sftp: %s exceeds %d bytes", remotePath, limit)
	}
	return buf, nil
}

// Close shuts down SFTP and SSH.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	sftpClient := c.sftp
	sshClient := c.ssh
	c.sftp = nil
	c.ssh = nil
	c.mu.Unlock()
	var err error
	if sftpClient != nil {
		err = sftpClient.Close()
	}
	if sshClient != nil {
		if cerr := sshClient.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

func (c *Client) keepalive() {
	interval := c.cfg.Keepalive.Std()
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.mu.Lock()
			client := c.ssh
			c.mu.Unlock()
			if client == nil {
				return
			}
			_, _, err := client.SendRequest("keepalive@openssh.com", true, nil)
			if err != nil {
				c.log.Debug("ssh keepalive failed", "error", err)
				c.mu.Lock()
				c.dead = true
				c.mu.Unlock()
				return
			}
		}
	}
}

func (c *Client) abort() {
	c.mu.Lock()
	c.dead = true
	sshClient := c.ssh
	c.mu.Unlock()
	if sshClient != nil {
		_ = sshClient.Close()
	}
}

// body streams one remote file. Abort closes the SSH connection because
// sftp.File.Read holds the file lock; calling File.Close from a watchdog
// deadlocks.
type body struct {
	file   *sftp.File
	client *Client
	once   sync.Once
}

func (b *body) Read(p []byte) (int, error) {
	n, err := b.file.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrNotExist) {
		b.client.mu.Lock()
		b.client.dead = true
		b.client.mu.Unlock()
	}
	return n, err
}

func (b *body) Abort() { b.client.abort() }

func (b *body) Close() error {
	var err error
	b.once.Do(func() {
		err = b.file.Close()
	})
	return err
}

func hostCallback(cfg config.Config) (ssh.HostKeyCallback, error) {
	if cfg.Insecure {
		return ssh.InsecureIgnoreHostKey(), nil
	}
	path := cfg.KnownHosts
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("sftp: known_hosts: %w", err)
		}
		path = filepath.Join(home, ".ssh", "known_hosts")
	}
	callback, err := knownhosts.New(path)
	if err != nil {
		return nil, fmt.Errorf("sftp: known_hosts %s: %w", path, err)
	}
	return callback, nil
}

func authMethods(cfg config.Config) ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod
	if cfg.PrivateKey != "" {
		pem, err := os.ReadFile(cfg.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("sftp: read key: %w", err)
		}
		var signer ssh.Signer
		if cfg.KeyPassphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(pem, []byte(cfg.KeyPassphrase))
		} else {
			signer, err = ssh.ParsePrivateKey(pem)
		}
		if err != nil {
			return nil, fmt.Errorf("sftp: parse key: %w", err)
		}
		methods = append(methods, ssh.PublicKeys(signer))
	}
	if cfg.Password != "" {
		methods = append(methods, ssh.Password(cfg.Password))
	}
	if len(methods) == 0 {
		return nil, errors.New("sftp: no authentication method")
	}
	return methods, nil
}

func clean(p string) string {
	p = strings.TrimSpace(p)
	p = strings.ReplaceAll(p, "\\", "/")
	if p == "" {
		return "/"
	}
	return p
}

func join(dir, name string) string {
	dir = strings.TrimRight(clean(dir), "/")
	if dir == "" {
		return "/" + name
	}
	return dir + "/" + name
}
