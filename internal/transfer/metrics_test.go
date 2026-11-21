package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/metrics"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
)

func TestEngineRecordsTransferMetrics(t *testing.T) {
	downloaded := map[string]string{"protocol": "ftp", "outcome": "downloaded"}
	skipped := map[string]string{"protocol": "ftp", "outcome": "skipped"}
	resume := map[string]string{"protocol": "ftp"}
	bytesLabels := map[string]string{"protocol": "ftp"}
	active := map[string]string{"protocol": "ftp", "lane": "small"}
	conns := map[string]string{"protocol": "ftp"}
	queue := map[string]string{"lane": "small"}
	listed := map[string]string{"protocol": "ftp", "result": "success"}

	beforeDownloaded := metrics.Sample("bezorg_transfers_total", downloaded)
	beforeSkipped := metrics.Sample("bezorg_transfers_total", skipped)
	beforeResume := metrics.Sample("bezorg_transfer_resumes_total", resume)
	beforeBytes := metrics.Sample("bezorg_transfer_bytes_total", bytesLabels)
	beforeActive := metrics.Sample("bezorg_transfers_active", active)
	beforeConns := metrics.Sample("bezorg_connections_active", conns)
	beforeQueue := metrics.Sample("bezorg_queue_files", queue)
	beforeLarge := metrics.Sample("bezorg_queue_files", map[string]string{"lane": "large"})
	beforeListed := metrics.Sample("bezorg_listing_duration_seconds", listed)

	store := &mem{files: map[string][]byte{
		"a.txt":     []byte("alpha"),
		"sub/b.txt": []byte("bravo-bravo"),
	}}
	cfg := testConfig(t)
	partial := filepath.Join(cfg.Local, "sub", "b.txt"+cfg.TempSuffix)
	if err := os.MkdirAll(filepath.Dir(partial), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partial, []byte("bravo"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Local, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := Run(context.Background(), Request{Config: cfg, Dial: store.dial})
	if err != nil {
		t.Fatal(err)
	}
	if result.FilesDone != 2 || result.FilesSkipped != 1 {
		t.Fatalf("result = %+v", result)
	}
	if got := metrics.Sample("bezorg_transfers_total", downloaded) - beforeDownloaded; got != 1 {
		t.Fatalf("downloaded = %v", got)
	}
	if got := metrics.Sample("bezorg_transfers_total", skipped) - beforeSkipped; got != 1 {
		t.Fatalf("skipped = %v", got)
	}
	if got := metrics.Sample("bezorg_transfer_resumes_total", resume) - beforeResume; got != 1 {
		t.Fatalf("resumes = %v", got)
	}
	if got := metrics.Sample("bezorg_transfer_bytes_total", bytesLabels) - beforeBytes; got != float64(len("bravo-bravo")-len("bravo")) {
		t.Fatalf("bytes = %v", got)
	}
	if got := metrics.Sample("bezorg_listing_duration_seconds", listed) - beforeListed; got != 1 {
		t.Fatalf("listings = %v", got)
	}
	if metrics.Sample("bezorg_transfers_active", active) != beforeActive {
		t.Fatalf("active transfers = %v, want %v", metrics.Sample("bezorg_transfers_active", active), beforeActive)
	}
	if metrics.Sample("bezorg_connections_active", conns) != beforeConns {
		t.Fatalf("active connections = %v, want %v", metrics.Sample("bezorg_connections_active", conns), beforeConns)
	}
	if metrics.Sample("bezorg_queue_files", queue) != beforeQueue || metrics.Sample("bezorg_queue_files", map[string]string{"lane": "large"}) != beforeLarge {
		t.Fatalf("queue leaked small=%v large=%v", metrics.Sample("bezorg_queue_files", queue), metrics.Sample("bezorg_queue_files", map[string]string{"lane": "large"}))
	}
	if metrics.Sample("bezorg_transfers_total", map[string]string{"protocol": "ftp", "outcome": "failed", "path": "sub/b.txt"}) != 0 {
		t.Fatal("file path was accepted as a label")
	}
}

func TestEngineMetricsWhileTransferIsActive(t *testing.T) {
	payload := bytes.Repeat([]byte("L"), 80)
	store := &mem{
		files:   map[string][]byte{"big.tif": payload, "a.txt": []byte("xy")},
		block:   "big.tif",
		release: make(chan struct{}),
		started: make(chan struct{}),
	}
	cfg := testConfig(t)
	active := map[string]string{"protocol": "ftp", "lane": "large"}
	conns := map[string]string{"protocol": "ftp"}
	beforeActive := metrics.Sample("bezorg_transfers_active", active)
	beforeConns := metrics.Sample("bezorg_connections_active", conns)
	beforeBytes := metrics.Sample("bezorg_transfer_bytes_total", map[string]string{"protocol": "ftp"})
	beforeDownloaded := metrics.Sample("bezorg_transfers_total", map[string]string{"protocol": "ftp", "outcome": "downloaded"})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = Run(context.Background(), Request{Config: cfg, Dial: store.dial})
	}()
	select {
	case <-store.started:
	case <-time.After(2 * time.Second):
		t.Fatal("large transfer did not start")
	}
	if got := metrics.Sample("bezorg_transfers_active", active); got < beforeActive+1 {
		t.Fatalf("large transfers active = %v, want at least %v", got, beforeActive+1)
	}
	if got := metrics.Sample("bezorg_connections_active", conns); got < beforeConns+1 {
		t.Fatalf("connections active = %v, want at least %v", got, beforeConns+1)
	}
	close(store.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not finish")
	}
	if metrics.Sample("bezorg_transfers_active", active) != beforeActive {
		t.Fatalf("large transfers active = %v", metrics.Sample("bezorg_transfers_active", active))
	}
	if metrics.Sample("bezorg_connections_active", conns) != beforeConns {
		t.Fatalf("connections active = %v", metrics.Sample("bezorg_connections_active", conns))
	}
	if got := metrics.Sample("bezorg_transfers_total", map[string]string{"protocol": "ftp", "outcome": "downloaded"}) - beforeDownloaded; got != 2 {
		t.Fatalf("downloaded = %v", got)
	}
	if got := metrics.Sample("bezorg_transfer_bytes_total", map[string]string{"protocol": "ftp"}) - beforeBytes; got != float64(len(payload)+2) {
		t.Fatalf("bytes = %v", got)
	}
}

func TestEngineMetricsCountRetries(t *testing.T) {
	store := &mem{files: map[string][]byte{"a.txt": []byte("hello")}, failDial: 1}
	cfg := testConfig(t)
	retryLabels := map[string]string{"protocol": "ftp", "scope": "dial", "kind": "transient"}
	errorLabels := map[string]string{"protocol": "ftp", "scope": "dial", "kind": "transient"}
	connErr := map[string]string{"protocol": "ftp", "result": "error"}
	beforeRetry := metrics.Sample("bezorg_retries_total", retryLabels)
	beforeErr := metrics.Sample("bezorg_errors_total", errorLabels)
	beforeConnErr := metrics.Sample("bezorg_connections_total", connErr)
	dials := 0
	_, err := Run(context.Background(), Request{
		Config: cfg,
		Dial: func(ctx context.Context, cfg config.Config) (remote.Session, error) {
			dials++
			return store.dial(ctx, cfg)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if dials != 3 {
		t.Fatalf("dials = %d, want list failure, list success, and one worker", dials)
	}
	if got := metrics.Sample("bezorg_retries_total", retryLabels) - beforeRetry; got != 1 {
		t.Fatalf("dial retries = %v", got)
	}
	if got := metrics.Sample("bezorg_errors_total", errorLabels) - beforeErr; got != 1 {
		t.Fatalf("dial errors = %v", got)
	}
	if got := metrics.Sample("bezorg_connections_total", connErr) - beforeConnErr; got != 1 {
		t.Fatalf("connection errors = %v", got)
	}

	payload := []byte("xyz")
	sum := sha256.Sum256(payload)
	checks := &mem{files: map[string][]byte{"c.bin": payload}}
	cfg = testConfig(t)
	cfg.Checksum = config.ChecksumRequire
	cfg.ChecksumAlgo = config.AlgoSHA256
	fileRetry := map[string]string{"protocol": "ftp", "scope": "file", "kind": "restart"}
	beforeFileRetry := metrics.Sample("bezorg_retries_total", fileRetry)
	beforeFileErr := metrics.Sample("bezorg_errors_total", fileRetry)
	beforeBytes := metrics.Sample("bezorg_transfer_bytes_total", map[string]string{"protocol": "ftp"})
	calls := 0
	result, err := Run(context.Background(), Request{
		Config: cfg,
		Dial:   checks.dial,
		Sum: func(context.Context, config.Config) (remote.Checksummer, error) {
			return sumFunc(func(string) (string, bool, error) {
				calls++
				if calls == 1 {
					return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true, nil
				}
				return hex.EncodeToString(sum[:]), true, nil
			}), nil
		},
	})
	if err != nil || result.FilesFailed != 0 {
		t.Fatalf("checksum retry result = %+v err %v", result, err)
	}
	if calls < 2 {
		t.Fatalf("checksum calls = %d", calls)
	}
	if got := metrics.Sample("bezorg_retries_total", fileRetry) - beforeFileRetry; got != 1 {
		t.Fatalf("file retries = %v", got)
	}
	if got := metrics.Sample("bezorg_errors_total", fileRetry) - beforeFileErr; got != 1 {
		t.Fatalf("file errors = %v", got)
	}
	if got := metrics.Sample("bezorg_transfer_bytes_total", map[string]string{"protocol": "ftp"}) - beforeBytes; got != float64(2*len(payload)) {
		t.Fatalf("retried bytes = %v, want %d", got, 2*len(payload))
	}

	cfg = testConfig(t)
	cfg.Retries = 5
	fatal := map[string]string{"protocol": "ftp", "scope": "dial", "kind": "fatal"}
	beforeFatal := metrics.Sample("bezorg_errors_total", fatal)
	beforeFatalRetry := metrics.Sample("bezorg_retries_total", fatal)
	fatalDials := 0
	_, err = Run(context.Background(), Request{
		Config: cfg,
		Dial: func(context.Context, config.Config) (remote.Session, error) {
			fatalDials++
			return nil, errors.New("login incorrect")
		},
	})
	if err == nil {
		t.Fatal("expected a fatal dial error")
	}
	if fatalDials != 1 {
		t.Fatalf("fatal dials = %d, want 1", fatalDials)
	}
	if got := metrics.Sample("bezorg_errors_total", fatal) - beforeFatal; got != 1 {
		t.Fatalf("fatal errors = %v", got)
	}
	if got := metrics.Sample("bezorg_retries_total", fatal) - beforeFatalRetry; got != 0 {
		t.Fatalf("fatal retries = %v", got)
	}
}
