package daemon

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/control"
	"github.com/GerhardOfRivia/bezorg/internal/ftptest"
	"github.com/GerhardOfRivia/bezorg/internal/job"
	"github.com/GerhardOfRivia/bezorg/internal/metrics"
)

func TestDaemonCompletesJobAfterClientExits(t *testing.T) {
	srv, err := ftptest.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.Put("/srv/dir/hello.txt", []byte("from-daemon"))

	root := t.TempDir()
	socketPath := filepath.Join(os.TempDir(), "bz-"+strconv.FormatInt(int64(os.Getpid()), 10)+".sock")
	defer os.Remove(socketPath)
	stateDir := filepath.Join(root, "state")
	local := filepath.Join(root, "out")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{
			StateDir: stateDir,
			Socket:   socketPath,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			Version:  "test",
			MaxJobs:  1,
		})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	client := control.NewClient(socketPath)
	host, portText, err := net.SplitHostPort(srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portText)
	cfg := config.Defaults()
	cfg.Protocol = config.ProtocolFTP
	cfg.Host = host
	cfg.Port = port
	cfg.User = srv.User
	cfg.Password = srv.Password
	cfg.Remote = "/srv"
	cfg.Local = local
	cfg.Checksum = config.ChecksumOff
	cfg.Concurrency = 2
	cfg.Keepalive = 0
	cfg.Retries = 3
	cfg.RetryBase = config.Duration(10 * time.Millisecond)
	cfg.RetryMax = config.Duration(50 * time.Millisecond)
	cfg.ConnectTimeout = config.Duration(2 * time.Second)
	cfg.StallTimeout = config.Duration(2 * time.Second)
	cfg.CommandTimeout = config.Duration(2 * time.Second)
	cfg.JobRetries = 0
	cfg, err = config.Validate(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var rec job.Record
	var submitErr error
	for time.Now().Before(deadline) {
		call, cancelCall := context.WithTimeout(context.Background(), time.Second)
		rec, submitErr = client.Submit(call, cfg)
		cancelCall()
		if submitErr == nil && rec.ID != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if submitErr != nil {
		t.Fatal(submitErr)
	}
	if rec.Config.Password != "****" {
		t.Fatalf("api returned a password: %+v", rec.Config)
	}

	var final job.Record
	waitUntil := time.Now().Add(15 * time.Second)
	for time.Now().Before(waitUntil) {
		call, cancelCall := context.WithTimeout(context.Background(), time.Second)
		final, err = client.Get(call, rec.ID)
		cancelCall()
		if err == nil && job.Terminal(final.State) {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if final.State != job.StateCompleted {
		t.Fatalf("job state %s error %s progress %+v", final.State, final.Error, final.Progress)
	}
	body, err := os.ReadFile(filepath.Join(local, "dir", "hello.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, []byte("from-daemon")) {
		t.Fatalf("file = %q", body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not stop")
	}
}

func TestWebDashboardRedactsPassword(t *testing.T) {
	root := t.TempDir()
	runtime := filepath.Join(os.TempDir(), "bzweb-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	if err := os.MkdirAll(runtime, 0o700); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(runtime)
	socketPath := filepath.Join(runtime, "bezorg.sock")
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	webAddress := probe.Addr().String()
	probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{
			StateDir:  filepath.Join(root, "state"),
			Socket:    socketPath,
			Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			Version:   "test",
			MaxJobs:   1,
			WebListen: webAddress,
		})
	}()

	tokenPath := socketPath + ".web-token"
	var token string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, readErr := os.ReadFile(tokenPath)
		if readErr == nil && strings.TrimSpace(string(data)) != "" {
			token = strings.TrimSpace(string(data))
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if token == "" {
		cancel()
		t.Fatal("web token was not written")
	}

	page, err := http.Get("http://" + webAddress + "/")
	if err != nil {
		t.Fatal(err)
	}
	pageBody, _ := io.ReadAll(page.Body)
	page.Body.Close()
	if page.StatusCode != http.StatusOK || !strings.Contains(string(pageBody), "<h1>bezorg</h1>") {
		t.Fatalf("dashboard %d %s", page.StatusCode, pageBody)
	}
	anonymous, err := http.Get("http://" + webAddress + "/api/v1/jobs")
	if err != nil {
		t.Fatal(err)
	}
	anonymous.Body.Close()
	if anonymous.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous status %d", anonymous.StatusCode)
	}

	const secret = "s3cret-web-ui"
	cfg := config.Defaults()
	cfg.Protocol = config.ProtocolFTP
	cfg.Host = "127.0.0.1"
	cfg.Port = 1
	cfg.User = "delivery"
	cfg.Password = secret
	cfg.Remote = "/incoming"
	cfg.Local = filepath.Join(root, "out")
	cfg.Checksum = config.ChecksumOff
	cfg.Concurrency = 1
	cfg.Keepalive = 0
	cfg.Retries = 1
	cfg.JobRetries = 0
	cfg.ConnectTimeout = config.Duration(200 * time.Millisecond)
	cfg.StallTimeout = config.Duration(time.Second)
	cfg.CommandTimeout = config.Duration(time.Second)
	cfg.RetryBase = config.Duration(10 * time.Millisecond)
	cfg.RetryMax = config.Duration(20 * time.Millisecond)
	client := control.NewClient(socketPath)
	var rec job.Record
	var submitErr error
	for time.Now().Before(deadline) {
		call, cancelCall := context.WithTimeout(context.Background(), time.Second)
		rec, submitErr = client.Submit(call, cfg)
		cancelCall()
		if submitErr == nil && rec.ID != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if submitErr != nil {
		t.Fatal(submitErr)
	}

	request, err := http.NewRequest(http.MethodGet, "http://"+webAddress+"/api/v1/jobs/"+rec.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("job status %d %s", response.StatusCode, body)
	}
	if strings.Contains(string(body), secret) || !strings.Contains(string(body), `"password": "****"`) {
		t.Fatalf("dashboard job body = %s", body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not stop")
	}
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("token file left behind: %v", err)
	}
}

func TestDaemonRejectsPublicMetricsAddress(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "bzmet-"+strconv.FormatInt(time.Now().UnixNano(), 10)+".sock")
	defer os.Remove(socketPath)
	err := Run(context.Background(), Options{
		StateDir:      t.TempDir(),
		Socket:        socketPath,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		MetricsListen: "192.0.2.10:9090",
	})
	if err == nil || !strings.Contains(err.Error(), "metrics") {
		t.Fatalf("error = %v", err)
	}
}

func TestDaemonExportsJobMetrics(t *testing.T) {
	srv, err := ftptest.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	const payload = "from-metrics"
	srv.Put("/srv/hello.txt", []byte(payload))

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsAddr := probe.Addr().String()
	probe.Close()

	root := t.TempDir()
	socketPath := filepath.Join(os.TempDir(), "bzmetjob-"+strconv.FormatInt(time.Now().UnixNano(), 10)+".sock")
	defer os.Remove(socketPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{
			StateDir:      filepath.Join(root, "state"),
			Socket:        socketPath,
			Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
			Version:       "metrics-test",
			MaxJobs:       1,
			MetricsListen: metricsAddr,
		})
	}()

	completed := map[string]string{"protocol": "ftp", "outcome": "completed"}
	beforeCompleted := metrics.Sample("bezorg_jobs_total", completed)
	beforeBytes := metrics.Sample("bezorg_transfer_bytes_total", map[string]string{"protocol": "ftp"})

	deadline := time.Now().Add(5 * time.Second)
	var scrape string
	for time.Now().Before(deadline) {
		response, scrapeErr := http.Get("http://" + metricsAddr + "/metrics")
		if scrapeErr == nil {
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK {
				scrape = string(body)
				break
			}
		}
		select {
		case err := <-done:
			t.Fatalf("daemon stopped before metrics were ready: %v", err)
		default:
		}
		time.Sleep(15 * time.Millisecond)
	}
	if scrape == "" || !strings.Contains(scrape, "bezorg_jobs_limit") || !strings.Contains(scrape, "go_goroutines") {
		t.Fatalf("metrics endpoint did not become ready: %q", scrape)
	}
	if metrics.Sample("bezorg_jobs_limit", nil) != 1 {
		t.Fatalf("jobs limit = %v", metrics.Sample("bezorg_jobs_limit", nil))
	}

	client := control.NewClient(socketPath)
	host, portText, err := net.SplitHostPort(srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portText)
	cfg := config.Defaults()
	cfg.Protocol = config.ProtocolFTP
	cfg.Host = host
	cfg.Port = port
	cfg.User = srv.User
	cfg.Password = srv.Password
	cfg.Remote = "/srv"
	cfg.Local = filepath.Join(root, "out")
	cfg.Checksum = config.ChecksumOff
	cfg.Concurrency = 1
	cfg.Keepalive = 0
	cfg.Retries = 2
	cfg.RetryBase = config.Duration(10 * time.Millisecond)
	cfg.RetryMax = config.Duration(20 * time.Millisecond)
	cfg.ConnectTimeout = config.Duration(2 * time.Second)
	cfg.StallTimeout = config.Duration(2 * time.Second)
	cfg.CommandTimeout = config.Duration(2 * time.Second)
	cfg.JobRetries = 0
	cfg, err = config.Validate(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var rec job.Record
	var submitErr error
	for time.Now().Before(deadline) {
		call, cancelCall := context.WithTimeout(context.Background(), time.Second)
		rec, submitErr = client.Submit(call, cfg)
		cancelCall()
		if submitErr == nil && rec.ID != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if submitErr != nil {
		t.Fatal(submitErr)
	}

	waitUntil := time.Now().Add(15 * time.Second)
	var final job.Record
	for time.Now().Before(waitUntil) {
		call, cancelCall := context.WithTimeout(context.Background(), time.Second)
		final, err = client.Get(call, rec.ID)
		cancelCall()
		if err == nil && job.Terminal(final.State) && metrics.Sample("bezorg_jobs_active", nil) == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if final.State != job.StateCompleted {
		t.Fatalf("job state %s error %s", final.State, final.Error)
	}
	if got := metrics.Sample("bezorg_jobs_total", completed) - beforeCompleted; got != 1 {
		t.Fatalf("completed jobs = %v", got)
	}
	if got := metrics.Sample("bezorg_transfer_bytes_total", map[string]string{"protocol": "ftp"}) - beforeBytes; got != float64(len(payload)) {
		t.Fatalf("bytes = %v", got)
	}
	if metrics.Sample("bezorg_jobs_active", nil) != 0 || metrics.Sample("bezorg_jobs_queued", nil) != 0 {
		t.Fatalf("active %v queued %v", metrics.Sample("bezorg_jobs_active", nil), metrics.Sample("bezorg_jobs_queued", nil))
	}
	if metrics.Sample("bezorg_build_info", map[string]string{"version": "metrics-test"}) != 1 {
		t.Fatal("build info was not set from the daemon version")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not stop")
	}
}
