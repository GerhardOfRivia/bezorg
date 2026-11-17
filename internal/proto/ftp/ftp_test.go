package ftp

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/ftptest"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
	"github.com/GerhardOfRivia/bezorg/internal/transfer"
)

func TestFTPDownloadResumeKeepaliveAndChecksum(t *testing.T) {
	srv, err := ftptest.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	payload := bytes.Repeat([]byte("imagery"), 900)
	srv.Put("/srv/small.txt", []byte("hello"))
	srv.Put("/srv/nest/scene.tif", payload)
	srv.ChunkDelay = 40 * time.Millisecond

	cfg := config.Defaults()
	host, portText, err := net.SplitHostPort(srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portText)
	cfg.Host = host
	cfg.Port = port
	cfg.Protocol = config.ProtocolFTP
	cfg.User = srv.User
	cfg.Password = srv.Password
	cfg.Remote = "/srv"
	cfg.Local = t.TempDir()
	cfg.Concurrency = 2
	cfg.LargeConcurrency = 1
	cfg.LargeMinBytes = 1024
	cfg.Checksum = config.ChecksumRequire
	cfg.ChecksumAlgo = config.AlgoSHA256
	cfg.Keepalive = config.Duration(80 * time.Millisecond)
	cfg.KeepaliveMinBytes = 1024
	cfg.StallTimeout = config.Duration(5 * time.Second)
	cfg.ConnectTimeout = config.Duration(3 * time.Second)
	cfg.CommandTimeout = config.Duration(5 * time.Second)
	cfg.Retries = 4
	cfg.RetryBase = config.Duration(20 * time.Millisecond)
	cfg.RetryMax = config.Duration(100 * time.Millisecond)
	cfg.ProbeZeroSize = true

	partial := filepath.Join(cfg.Local, "nest", "scene.tif"+cfg.TempSuffix)
	if err := os.MkdirAll(filepath.Dir(partial), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partial, payload[:100], 0o644); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result, err := transfer.Run(ctx, transfer.Request{
		Config: cfg,
		Logger: logger,
		Dial: func(ctx context.Context, cfg config.Config) (remote.Session, error) {
			return Dial(ctx, cfg, logger)
		},
		Sum: func(ctx context.Context, cfg config.Config) (remote.Checksummer, error) {
			return NewChecksummer(ctx, cfg, logger)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FilesFailed != 0 || result.FilesDone != 2 {
		t.Fatalf("result = %+v", result)
	}
	got, err := os.ReadFile(filepath.Join(cfg.Local, "nest", "scene.tif"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("scene.tif length %d, want %d", len(got), len(payload))
	}
	offsets := srv.RESTOffsets()
	resumed := false
	for _, offset := range offsets {
		if offset == 100 {
			resumed = true
		}
	}
	if !resumed {
		t.Fatalf("REST offsets = %v, want 100", offsets)
	}
	if srv.Noops.Load() == 0 {
		t.Fatal("expected a keepalive NOOP during the slow transfer")
	}
}

func TestFTPLoginLimitIsRetried(t *testing.T) {
	srv, err := ftptest.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.Put("/srv/a.txt", []byte("ok"))
	srv.FailLogins.Store(1)

	cfg := config.Defaults()
	host, portText, _ := net.SplitHostPort(srv.Addr)
	port, _ := strconv.Atoi(portText)
	cfg.Host = host
	cfg.Port = port
	cfg.Protocol = config.ProtocolFTP
	cfg.User = srv.User
	cfg.Password = srv.Password
	cfg.Remote = "/srv"
	cfg.Local = t.TempDir()
	cfg.Checksum = config.ChecksumOff
	cfg.Concurrency = 1
	cfg.Retries = 4
	cfg.RetryBase = config.Duration(10 * time.Millisecond)
	cfg.RetryMax = config.Duration(50 * time.Millisecond)
	cfg.ConnectTimeout = config.Duration(2 * time.Second)
	cfg.CommandTimeout = config.Duration(2 * time.Second)
	cfg.StallTimeout = config.Duration(2 * time.Second)
	cfg.Keepalive = 0

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	result, err := transfer.Run(context.Background(), transfer.Request{
		Config: cfg,
		Logger: logger,
		Dial: func(ctx context.Context, cfg config.Config) (remote.Session, error) {
			return Dial(ctx, cfg, logger)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FilesDone != 1 {
		t.Fatalf("result = %+v", result)
	}
}
