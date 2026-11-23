package webui

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/control"
	"github.com/GerhardOfRivia/bezorg/internal/events"
	"github.com/GerhardOfRivia/bezorg/internal/job"
	"github.com/GerhardOfRivia/bezorg/internal/transfer"
	"github.com/GerhardOfRivia/bezorg/internal/webhook"
)

func TestListenAddressRestrictions(t *testing.T) {
	svc := &fakeService{}
	for _, address := range []string{"192.0.2.1:8080", "dashboard.example:8080", "127.0.0.1"} {
		_, err := NewServer(address, filepath.Join(t.TempDir(), "token"), svc, discardLogger(), false)
		if err == nil {
			t.Fatalf("address %s was accepted", address)
		}
	}

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := NewServer("127.0.0.1:0", filepath.Join(dir, "token"), svc, discardLogger(), false)
	if err == nil || !strings.Contains(err.Error(), "private") {
		t.Fatalf("open token directory error = %v", err)
	}
}

func TestDashboardRequiresTokenAndRedactsSecrets(t *testing.T) {
	const secret = "s3cret-web-ui"
	svc := &fakeService{
		system: control.System{Version: "test", Active: 1, Queued: 2, StartedAt: time.Unix(1, 0).UTC()},
		jobs: map[string]job.Record{
			"abc123": {
				ID:    "abc123",
				State: job.StateRunning,
				Config: config.Config{
					Protocol: config.ProtocolFTP,
					Host:     "ftp.example",
					User:     "delivery",
					Password: secret,
					Remote:   "/incoming",
					Local:    "/data/incoming",
				},
				Progress: transfer.Snapshot{FilesTotal: 4, FilesDone: 1, BytesTotal: 100, BytesDone: 25},
			},
		},
		logs: map[string][]string{"abc123": {"listed /incoming", "got hello.txt"}},
	}
	server := startServer(t, svc, "127.0.0.1:0", false)
	token := readToken(t, server)
	base := "http://" + server.Address()

	page, pageBody := doRequest(t, http.MethodGet, base+"/", "", "")
	if page.StatusCode != http.StatusOK || !strings.Contains(pageBody, "<h1>bezorg</h1>") || !strings.Contains(pageBody, "/dashboard.js") {
		t.Fatalf("page %d %s", page.StatusCode, pageBody)
	}
	if strings.Contains(pageBody, "http://") || strings.Contains(pageBody, "https://") {
		t.Fatalf("page links off-origin: %s", pageBody)
	}
	if page.Header.Get("Content-Security-Policy") == "" {
		t.Fatal("missing content security policy")
	}
	css, _ := doRequest(t, http.MethodGet, base+"/dashboard.css", "", "")
	js, jsBody := doRequest(t, http.MethodGet, base+"/dashboard.js", "", "")
	if css.StatusCode != http.StatusOK || js.StatusCode != http.StatusOK || !strings.Contains(jsBody, "/api/v1/jobs") {
		t.Fatalf("assets css %d js %d", css.StatusCode, js.StatusCode)
	}

	denied, _ := doRequest(t, http.MethodGet, base+"/api/v1/jobs", "", "")
	if denied.StatusCode != http.StatusUnauthorized || denied.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("anonymous jobs %d", denied.StatusCode)
	}
	bad, _ := doRequest(t, http.MethodGet, base+"/api/v1/jobs", "Bearer wrong-token", "")
	if bad.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad token %d", bad.StatusCode)
	}

	ok, body := doRequest(t, http.MethodGet, base+"/api/v1/jobs", "Bearer "+token, "")
	if ok.StatusCode != http.StatusOK {
		t.Fatalf("jobs %d %s", ok.StatusCode, body)
	}
	if strings.Contains(body, secret) || !strings.Contains(body, `"password": "****"`) {
		t.Fatalf("jobs leaked or dropped the secret marker: %s", body)
	}
	if !strings.Contains(body, "ftp.example") || !strings.Contains(body, "/incoming") {
		t.Fatalf("jobs dropped public fields: %s", body)
	}

	one, oneBody := doRequest(t, http.MethodGet, base+"/api/v1/jobs/abc123", "Bearer "+token, "")
	if one.StatusCode != http.StatusOK || strings.Contains(oneBody, secret) {
		t.Fatalf("job %d %s", one.StatusCode, oneBody)
	}
	logs, logsBody := doRequest(t, http.MethodGet, base+"/api/v1/jobs/abc123/logs?after=1", "Bearer "+token, "")
	if logs.StatusCode != http.StatusOK || !strings.Contains(logsBody, "got hello.txt") || strings.Contains(logsBody, "listed /incoming") {
		t.Fatalf("logs %d %s", logs.StatusCode, logsBody)
	}

	missing, missingBody := doRequest(t, http.MethodGet, base+"/api/v1/jobs/missing", "Bearer "+token, "")
	if missing.StatusCode != http.StatusNotFound || !strings.Contains(missingBody, "job not found") {
		t.Fatalf("missing %d %s", missing.StatusCode, missingBody)
	}
	conflict, _ := doRequest(t, http.MethodPost, base+"/api/v1/jobs/abc123/retry", "Bearer "+token, "")
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("retry %d", conflict.StatusCode)
	}
	canceled, _ := doRequest(t, http.MethodPost, base+"/api/v1/jobs/abc123/cancel", "Bearer "+token, "")
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel %d", canceled.StatusCode)
	}
	if got := svc.canceledID(); got != "abc123" {
		t.Fatalf("canceled %q", got)
	}

	alien, _ := doRequest(t, http.MethodGet, base+"/", "", "dashboard.example")
	if alien.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("host status %d", alien.StatusCode)
	}
}

func TestPublicReadStillProtectsActions(t *testing.T) {
	svc := &fakeService{jobs: map[string]job.Record{
		"job1": {ID: "job1", State: job.StateFailed, Config: config.Config{Password: "s3cret-web-ui"}},
	}}
	server := startServer(t, svc, "127.0.0.1:0", true)
	base := "http://" + server.Address()

	jobs, body := doRequest(t, http.MethodGet, base+"/api/v1/jobs", "", "")
	if jobs.StatusCode != http.StatusOK || strings.Contains(body, "s3cret-web-ui") {
		t.Fatalf("public jobs %d %s", jobs.StatusCode, body)
	}
	system, systemBody := doRequest(t, http.MethodGet, base+"/api/v1/system", "", "")
	if system.StatusCode != http.StatusOK || !strings.Contains(systemBody, `"public_read": true`) {
		t.Fatalf("system %d %s", system.StatusCode, systemBody)
	}
	denied, _ := doRequest(t, http.MethodPost, base+"/api/v1/jobs/job1/retry", "", "")
	if denied.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous retry %d", denied.StatusCode)
	}
	bad, _ := doRequest(t, http.MethodGet, base+"/api/v1/jobs", "Bearer nope", "")
	if bad.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad token became anonymous: %d", bad.StatusCode)
	}

	token := readToken(t, server)
	retried, _ := doRequest(t, http.MethodPost, base+"/api/v1/jobs/job1/retry", "Bearer "+token, "")
	if retried.StatusCode != http.StatusOK || svc.retriedID() != "job1" {
		t.Fatalf("retry %d id %s", retried.StatusCode, svc.retriedID())
	}
}

func TestWildcardAllowsIPHostsOnly(t *testing.T) {
	server := startServer(t, &fakeService{}, "0.0.0.0:0", false)
	_, port, err := net.SplitHostPort(server.Address())
	if err != nil {
		t.Fatal(err)
	}
	page, _ := doRequest(t, http.MethodGet, "http://127.0.0.1:"+port+"/", "", "192.0.2.1:"+port)
	if page.StatusCode != http.StatusOK {
		t.Fatalf("ip host %d", page.StatusCode)
	}
	name, _ := doRequest(t, http.MethodGet, "http://127.0.0.1:"+port+"/", "", "dashboard.example")
	if name.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("name host %d", name.StatusCode)
	}
}

func TestJobsFilterExternalID(t *testing.T) {
	svc := &fakeService{jobs: map[string]job.Record{
		"one": {ID: "one", State: job.StateQueued, ExternalID: "batch-42", Config: config.Config{Host: "ftp.example", Remote: "/a", Local: "/data/a"}},
		"two": {ID: "two", State: job.StateQueued, ExternalID: "other", Config: config.Config{Host: "ftp.example", Remote: "/b", Local: "/data/b"}},
	}}
	server := startServer(t, svc, "127.0.0.1:0", false)
	token := readToken(t, server)
	response, body := doRequest(t, http.MethodGet, "http://"+server.Address()+"/api/v1/jobs?external_id=batch-42", "Bearer "+token, "")
	if response.StatusCode != http.StatusOK || !strings.Contains(body, `"id": "one"`) || strings.Contains(body, `"id": "two"`) {
		t.Fatalf("status %d body %s", response.StatusCode, body)
	}
	response, body = doRequest(t, http.MethodGet, "http://"+server.Address()+"/api/v1/jobs?external_id="+strings.Repeat("x", 257), "Bearer "+token, "")
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized filter status %d body %s", response.StatusCode, body)
	}
}

func TestCloseRemovesToken(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer("127.0.0.1:0", filepath.Join(dir, "web-token"), &fakeService{}, discardLogger(), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(server.TokenPath()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- server.Serve(ctx) }()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return")
	}
	if _, err := os.Stat(server.TokenPath()); !os.IsNotExist(err) {
		t.Fatalf("token file err = %v", err)
	}
}

type fakeService struct {
	mu       sync.Mutex
	system   control.System
	jobs     map[string]job.Record
	logs     map[string][]string
	feed     []events.Event
	hooks    *webhook.Store
	canceled string
	retried  string
}

func (f *fakeService) Submit(ctx context.Context, cfg config.Config) (job.Record, error) {
	return f.SubmitDownload(ctx, cfg, 0, nil)
}

func (f *fakeService) SubmitDownload(_ context.Context, cfg config.Config, priority int, metadata json.RawMessage) (job.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cfg.ExternalID != "" {
		for _, rec := range f.jobs {
			if rec.ExternalID == cfg.ExternalID {
				return job.Public(rec), job.ErrExternalIDInUse
			}
		}
	}
	rec := job.Record{
		ID: "created", State: job.StateQueued, ExternalID: strings.TrimSpace(cfg.ExternalID),
		Config: cfg.Redacted(), Priority: priority, Metadata: metadata,
	}
	if f.jobs == nil {
		f.jobs = map[string]job.Record{}
	}
	f.jobs[rec.ID] = rec
	return job.Public(rec), nil
}

func (f *fakeService) List(_ context.Context, externalID string) ([]job.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]job.Record, 0, len(f.jobs))
	for _, rec := range f.jobs {
		if externalID != "" && rec.ExternalID != externalID {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

func (f *fakeService) Get(_ context.Context, selector string) (job.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.jobs[selector]
	if !ok {
		return job.Record{}, job.ErrNotFound
	}
	return rec, nil
}

func (f *fakeService) Cancel(_ context.Context, selector string) (job.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.jobs[selector]
	if !ok {
		return job.Record{}, job.ErrNotFound
	}
	f.canceled = selector
	rec.State = job.StateCanceled
	f.jobs[selector] = rec
	return rec, nil
}

func (f *fakeService) Retry(_ context.Context, selector string) (job.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.jobs[selector]
	if !ok {
		return job.Record{}, job.ErrNotFound
	}
	if rec.State != job.StateFailed && rec.State != job.StateCanceled && rec.State != job.StateInterrupted {
		return job.Record{}, errors.New("job " + selector + " is " + rec.State + " and cannot be retried")
	}
	f.retried = selector
	rec.State = job.StateQueued
	f.jobs[selector] = rec
	return rec, nil
}

func (f *fakeService) Logs(_ context.Context, selector string, after int) ([]string, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.jobs[selector]; !ok {
		return nil, after, job.ErrNotFound
	}
	lines := f.logs[selector]
	if after < 0 {
		after = 0
	}
	if after > len(lines) {
		after = len(lines)
	}
	return lines[after:], after + len(lines[after:]), nil
}

func (f *fakeService) System(context.Context) (control.System, error) {
	return f.system, nil
}

func (f *fakeService) CreateWebhook(ctx context.Context, params webhook.Params) (webhook.Hook, error) {
	if f.hooks == nil {
		return webhook.Hook{}, errors.New("webui test: webhooks are not used")
	}
	return f.hooks.Create(ctx, params)
}

func (f *fakeService) ListWebhooks(ctx context.Context) ([]webhook.Hook, error) {
	if f.hooks == nil {
		return []webhook.Hook{}, nil
	}
	return f.hooks.List(ctx)
}

func (f *fakeService) DeleteWebhook(ctx context.Context, id int64) error {
	if f.hooks == nil {
		return webhook.ErrNotFound
	}
	return f.hooks.Delete(ctx, id)
}

func (f *fakeService) ListWebhookDeliveries(ctx context.Context, id int64, status string, limit int) ([]webhook.Delivery, error) {
	if f.hooks == nil {
		return nil, webhook.ErrNotFound
	}
	return f.hooks.Deliveries(ctx, id, status, limit)
}

func (f *fakeService) Events(_ context.Context, after int64, limit int) ([]events.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	out := make([]events.Event, 0)
	for _, event := range f.feed {
		if event.Seq <= after {
			continue
		}
		out = append(out, event)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (f *fakeService) canceledID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.canceled
}

func (f *fakeService) retriedID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.retried
}

func startServer(t *testing.T, svc control.Service, address string, public bool) *Server {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(address, filepath.Join(dir, "web-token"), svc, discardLogger(), public)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- server.Serve(ctx) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
		cancel()
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("serve did not return")
		}
	})
	return server
}

func readToken(t *testing.T, server *Server) string {
	t.Helper()
	info, err := os.Stat(server.TokenPath())
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("token mode %o", info.Mode().Perm())
	}
	data, err := os.ReadFile(server.TokenPath())
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(data))
}

func doRequest(t *testing.T, method, url, authorization, host string) (*http.Response, string) {
	t.Helper()
	request, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	if host != "" {
		request.Host = host
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, string(body)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
