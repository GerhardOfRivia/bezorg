package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
)

type mem struct {
	mu       sync.Mutex
	files    map[string][]byte
	opens    []int64
	failDial int
	release  chan struct{}
	started  chan struct{}
	block    string
}

func (m *mem) dial(context.Context, config.Config) (remote.Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failDial > 0 {
		m.failDial--
		return nil, syscall.ECONNRESET
	}
	return &memSession{mem: m}, nil
}

type memSession struct{ mem *mem }

func (s *memSession) List(context.Context, string) (remote.Listing, error) {
	s.mem.mu.Lock()
	defer s.mem.mu.Unlock()
	var files []remote.File
	for path, data := range s.mem.files {
		files = append(files, remote.File{Rel: path, Remote: "/" + path, Size: int64(len(data))})
	}
	return remote.Listing{Files: files}, nil
}

func (s *memSession) OpenAt(_ context.Context, remotePath string, offset int64) (io.ReadCloser, error) {
	s.mem.mu.Lock()
	s.mem.opens = append(s.mem.opens, offset)
	data := append([]byte(nil), s.mem.files[remotePath[1:]]...)
	block := s.mem.block != "" && remotePath[1:] == s.mem.block
	s.mem.mu.Unlock()
	if offset > int64(len(data)) {
		return nil, errors.New("offset past end")
	}
	body := &memBody{data: data[offset:]}
	if block {
		body.release = s.mem.release
		body.started = s.mem.started
	}
	return body, nil
}

func (s *memSession) ReadSmall(_ context.Context, remotePath string, limit int64) ([]byte, error) {
	s.mem.mu.Lock()
	defer s.mem.mu.Unlock()
	data, ok := s.mem.files[remotePath[1:]]
	if !ok {
		return nil, os.ErrNotExist
	}
	if int64(len(data)) > limit {
		return nil, errors.New("too big")
	}
	return append([]byte(nil), data...), nil
}

func (s *memSession) Close() error { return nil }

type memBody struct {
	data    []byte
	release <-chan struct{}
	started chan struct{}
	once    sync.Once
}

func (b *memBody) Read(p []byte) (int, error) {
	if b.release != nil {
		b.once.Do(func() {
			if b.started != nil {
				close(b.started)
			}
		})
		select {
		case <-b.release:
		case <-time.After(2 * time.Second):
			return 0, errors.New("blocked reader timed out")
		}
		b.release = nil
	}
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

func (b *memBody) Close() error { return nil }

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Defaults()
	cfg.Host = "files.example.test"
	cfg.Protocol = config.ProtocolFTP
	cfg.Port = 21
	cfg.User = "user"
	cfg.Password = "secret"
	cfg.Remote = "/drop"
	cfg.Local = t.TempDir()
	cfg.Checksum = config.ChecksumOff
	cfg.Retries = 4
	cfg.RetryBase = config.Duration(time.Millisecond)
	cfg.RetryMax = config.Duration(20 * time.Millisecond)
	cfg.ConnectTimeout = config.Duration(time.Second)
	cfg.StallTimeout = config.Duration(time.Second)
	cfg.CommandTimeout = config.Duration(time.Second)
	cfg.Keepalive = 0
	cfg.Concurrency = 4
	cfg.LargeConcurrency = 1
	cfg.LargeMinBytes = 64
	cfg.JobRetries = 0
	return cfg
}

func TestEngineMirrorsTreeResumesAndSkips(t *testing.T) {
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
	if result.FilesDone != 2 || result.FilesSkipped != 1 || result.FilesFailed != 0 {
		t.Fatalf("result = %+v", result)
	}
	got, err := os.ReadFile(filepath.Join(cfg.Local, "sub", "b.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("bravo-bravo")) {
		t.Fatalf("resumed file = %q", got)
	}
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial still present: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.opens) != 1 || store.opens[0] != int64(len("bravo")) {
		t.Fatalf("opens = %v, want a single resume at offset %d", store.opens, len("bravo"))
	}
}

func TestEngineRetriesDialAndVerifiesChecksum(t *testing.T) {
	payload := []byte("checksum-me")
	sum := sha256.Sum256(payload)
	store := &mem{files: map[string][]byte{"c.bin": payload}, failDial: 1}
	cfg := testConfig(t)
	cfg.Checksum = config.ChecksumRequire
	cfg.ChecksumAlgo = config.AlgoSHA256
	calls := 0
	result, err := Run(context.Background(), Request{
		Config: cfg,
		Dial:   store.dial,
		Sum: func(context.Context, config.Config) (remote.Checksummer, error) {
			return sumFunc(func(path string) (string, bool, error) {
				calls++
				if calls == 1 {
					return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true, nil
				}
				return hex.EncodeToString(sum[:]), true, nil
			}), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FilesFailed != 0 || result.FilesDone != 1 {
		t.Fatalf("result = %+v", result)
	}
	if calls < 2 {
		t.Fatalf("checksum calls = %d, want a mismatch retry", calls)
	}
}

func TestEngineKeepsSmallFilesMoving(t *testing.T) {
	payload := bytes.Repeat([]byte("L"), 80)
	store := &mem{
		files: map[string][]byte{
			"big.tif": payload,
			"s1.xml":  []byte("1"),
			"s2.xml":  []byte("2"),
			"s3.xml":  []byte("3"),
		},
		block:   "big.tif",
		release: make(chan struct{}),
		started: make(chan struct{}),
	}
	cfg := testConfig(t)
	cfg.Concurrency = 3
	cfg.LargeConcurrency = 1
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
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ready := 0
		for _, name := range []string{"s1.xml", "s2.xml", "s3.xml"} {
			if _, err := os.Stat(filepath.Join(cfg.Local, name)); err == nil {
				ready++
			}
		}
		if ready == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, name := range []string{"s1.xml", "s2.xml", "s3.xml"} {
		if _, err := os.Stat(filepath.Join(cfg.Local, name)); err != nil {
			t.Fatalf("small file %s was not downloaded while the large file was in flight: %v", name, err)
		}
	}
	close(store.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not finish after the large file was released")
	}
}

func TestExternalIDSidecarIsPublishedBeforeTheFinalRename(t *testing.T) {
	store := &mem{files: map[string][]byte{"a.txt": []byte("alpha")}}
	cfg := testConfig(t)
	cfg.ExternalID = " batch-42 "
	final := filepath.Join(cfg.Local, "a.txt")
	if err := os.Mkdir(final, 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := Run(context.Background(), Request{Config: cfg, Dial: store.dial, JobID: "job-1"})
	if err == nil {
		t.Fatal("rename onto an existing directory succeeded")
	}
	body, err := os.ReadFile(final + ".bezorg-id")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"external_id": "batch-42"`)) || !bytes.Contains(body, []byte(`"job_id": "job-1"`)) || !bytes.Contains(body, []byte(`"remote": "/a.txt"`)) {
		t.Fatalf("sidecar = %s", body)
	}
	if _, err := os.Stat(final + cfg.TempSuffix); err != nil {
		t.Fatalf("partial missing after failed rename: %v", err)
	}
	if _, err := os.Stat(final + ".bezorg-id" + cfg.TempSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sidecar temp still present: %v", err)
	}

	if err := os.RemoveAll(final); err != nil {
		t.Fatal(err)
	}
	result, err := Run(context.Background(), Request{Config: cfg, Dial: store.dial, JobID: "job-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.FilesDone != 1 || result.FilesFailed != 0 {
		t.Fatalf("result = %+v", result)
	}
	got, err := os.ReadFile(final)
	if err != nil || string(got) != "alpha" {
		t.Fatalf("file = %q %v", got, err)
	}
	if _, err := os.Stat(final + ".bezorg-id"); err != nil {
		t.Fatal(err)
	}

	cfg.ExternalID = ""
	plainDir := t.TempDir()
	cfg.Local = plainDir
	if _, err := Run(context.Background(), Request{Config: cfg, Dial: store.dial}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(plainDir, "a.txt.bezorg-id")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sidecar written without an external id: %v", err)
	}
}

func TestEngineDryRunWritesNothing(t *testing.T) {
	store := &mem{files: map[string][]byte{"only.txt": []byte("x")}}
	cfg := testConfig(t)
	cfg.DryRun = true
	result, err := Run(context.Background(), Request{Config: cfg, Dial: store.dial})
	if err != nil {
		t.Fatal(err)
	}
	if !result.DryRun || result.FilesTotal != 1 {
		t.Fatalf("result = %+v", result)
	}
	entries, err := os.ReadDir(cfg.Local)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 || len(store.opens) != 0 {
		t.Fatalf("dry-run wrote %d entries and opened %d files", len(entries), len(store.opens))
	}
}

func TestCopyRespectsBandwidth(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 48*1024)
	lim := newLimiter(16 * 1024)
	start := time.Now()
	n, err := copyStream(context.Background(), io.Discard, io.NopCloser(bytes.NewReader(payload)), 0, lim, 8*1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("copied %d", n)
	}
	if time.Since(start) < 500*time.Millisecond {
		t.Fatalf("copy finished in %s, bandwidth limit was not applied", time.Since(start))
	}
}

func newLimiter(perSec int) *rateLimiter {
	return rateNew(perSec)
}

type sumFunc func(path string) (string, bool, error)

func (f sumFunc) Sum(_ context.Context, path, _ string) (string, bool, error) {
	return f(path)
}
func (f sumFunc) Close() error { return nil }
