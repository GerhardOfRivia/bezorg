package sftp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	pkgsftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func TestSFTPDownloadAndResume(t *testing.T) {
	src := t.TempDir()
	payload := bytes.Repeat([]byte("ntf"), 3000)
	if err := os.MkdirAll(filepath.Join(src, "img"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "img", "frame.ntf"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "img", "note.txt"), []byte("meta"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(src, "img", "note.txt"), filepath.Join(src, "img", "note.link")); err != nil {
		t.Fatal(err)
	}
	addr, stop := serveSSH(t, "user", "secret")
	defer stop()

	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portText)
	cfg := config.Defaults()
	cfg.Protocol = config.ProtocolSFTP
	cfg.Host = host
	cfg.Port = port
	cfg.User = "user"
	cfg.Password = "secret"
	cfg.Insecure = true
	cfg.Remote = src
	cfg.Local = t.TempDir()
	cfg.Checksum = config.ChecksumOff
	cfg.Concurrency = 2
	cfg.Keepalive = config.Duration(time.Hour)
	cfg.ConnectTimeout = config.Duration(3 * time.Second)
	cfg.StallTimeout = config.Duration(3 * time.Second)
	cfg.CommandTimeout = config.Duration(3 * time.Second)

	partial := filepath.Join(cfg.Local, "img", "frame.ntf"+cfg.TempSuffix)
	if err := os.MkdirAll(filepath.Dir(partial), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partial, payload[:120], 0o644); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client, err := Dial(context.Background(), cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	listing, err := client.List(context.Background(), cfg.Remote)
	if err != nil {
		t.Fatal(err)
	}
	foundLink := false
	var frame remoteSize
	for _, file := range listing.Files {
		if filepath.Base(file.Rel) == "note.link" {
			foundLink = true
		}
		if filepath.Base(file.Rel) == "frame.ntf" {
			frame = remoteSize{path: file.Remote, size: file.Size}
		}
	}
	if foundLink {
		t.Fatal("symlink was included in the listing")
	}
	if frame.size != int64(len(payload)) {
		t.Fatalf("frame size %d", frame.size)
	}
	body, err := client.OpenAt(context.Background(), frame.path, 120)
	if err != nil {
		t.Fatal(err)
	}
	rest, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		t.Fatal(err)
	}
	got := append(append([]byte{}, payload[:120]...), rest...)
	if !bytes.Equal(got, payload) {
		t.Fatalf("resumed length %d want %d", len(got), len(payload))
	}
	if err := client.Close(); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

type remoteSize struct {
	path string
	size int64
}

func serveSSH(t *testing.T, user, password string) (string, func()) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	serverCfg := &ssh.ServerConfig{
		PasswordCallback: func(meta ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if meta.User() == user && string(pass) == password {
				return nil, nil
			}
			return nil, errors.New("denied")
		},
	}
	serverCfg.AddHostKey(signer)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleSSH(conn, serverCfg)
		}
	}()
	return ln.Addr().String(), func() { _ = ln.Close() }
}

func handleSSH(conn net.Conn, cfg *ssh.ServerConfig) {
	defer conn.Close()
	sshConn, chans, reqs, err := ssh.NewServerConn(conn, cfg)
	if err != nil {
		return
	}
	defer sshConn.Close()
	go ssh.DiscardRequests(reqs)
	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "unsupported")
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			continue
		}
		go func() {
			defer channel.Close()
			for req := range requests {
				if req.Type == "subsystem" && bytes.Contains(req.Payload, []byte("sftp")) {
					_ = req.Reply(true, nil)
					server, err := pkgsftp.NewServer(channel)
					if err != nil {
						return
					}
					_ = server.Serve()
					return
				}
				_ = req.Reply(false, nil)
			}
		}()
	}
}
