package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/events"
	"github.com/GerhardOfRivia/bezorg/internal/metrics"
)

func TestSignHeaderMatchesTheBody(t *testing.T) {
	stamp := time.Unix(1_700_000_000, 0).UTC()
	body := []byte(`{"seq":1,"type":"download.succeeded"}`)
	got := Sign("super-secret-value", body, stamp)
	const want = "t=1700000000,v1=100ec04141a28b22a98c372fccceedcd42b3fdc1506126feefb883d8030a9cd2"
	if got != want {
		t.Fatalf("signature = %s", got)
	}
	if Sign("other-secret-value!", body, stamp) == got {
		t.Fatal("different secrets produced the same signature")
	}
}

func TestWebhookSecretIsStoredButNotReturned(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const secret = "super-secret-value"
	hook, err := store.Create(context.Background(), Params{URL: "https://agent.example/hooks", Secret: secret, Events: []string{events.DownloadSucceeded}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(hook)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), secret) {
		t.Fatalf("secret returned: %s", encoded)
	}
	raw, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(store.path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v %v", info, err)
	}
	if !bytes.Contains(raw, []byte(secret)) {
		t.Fatal("secret was not stored")
	}
	event := events.Event{Seq: 4, Type: events.DownloadSucceeded, JobID: "job-1", ExternalID: "batch-7", Remote: "/drop/a.txt", Local: "/data/a.txt", Status: "completed", Bytes: 9, CreatedAt: time.Unix(3, 0).UTC()}
	if err := store.Enqueue(event); err != nil {
		t.Fatal(err)
	}
	pending, err := store.Deliveries(context.Background(), hook.ID, "pending", 10)
	if err != nil || len(pending) != 1 || pending[0].EventType != events.DownloadSucceeded || !strings.Contains(pending[0].Payload, "batch-7") || strings.Contains(pending[0].Payload, secret) {
		t.Fatalf("pending = %+v %v", pending, err)
	}
	if _, err := store.Create(context.Background(), Params{URL: "ftp://files.example/hook", Secret: secret}); err == nil {
		t.Fatal("non-http url accepted")
	}
	if err := store.Delete(context.Background(), hook.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Deliveries(context.Background(), hook.ID, "", 10); err == nil {
		t.Fatal("deleted webhook still listed deliveries")
	}
}

func TestDeliveryRetriesThenDeadLetters(t *testing.T) {
	previous := backoffFor
	backoffFor = func(int) time.Duration { return time.Millisecond }
	t.Cleanup(func() { backoffFor = previous })

	const secret = "super-secret-value"
	var calls int
	var deliveryID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		calls++
		if r.Header.Get("X-bezorg-Event") != events.DownloadQueued || r.Header.Get("X-bezorg-Delivery") == "" || !strings.HasPrefix(r.Header.Get("X-bezorg-Signature"), "t=") || !strings.Contains(r.Header.Get("X-bezorg-Signature"), ",v1=") {
			t.Errorf("headers = %v", r.Header)
		}
		if strings.Contains(string(body), secret) {
			t.Errorf("secret in body")
		}
		if deliveryID == "" {
			deliveryID = r.Header.Get("X-bezorg-Delivery")
		} else if r.Header.Get("X-bezorg-Delivery") != deliveryID {
			t.Errorf("delivery id changed from %s to %s", deliveryID, r.Header.Get("X-bezorg-Delivery"))
		}
		http.Error(w, "no", http.StatusBadGateway)
	}))
	defer server.Close()

	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), Params{URL: server.URL, Secret: secret, MaxAttempts: 2}); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(events.Event{Seq: 1, Type: events.DownloadQueued, JobID: "job-1", Status: "queued", CreatedAt: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if err := deliverDue(context.Background(), store, client, logger); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := deliverDue(context.Background(), store, client, logger); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d", calls)
	}
	hooks, err := store.List(context.Background())
	if err != nil || len(hooks) != 1 {
		t.Fatal(err)
	}
	dead, err := store.Deliveries(context.Background(), hooks[0].ID, "dead", 10)
	if err != nil || len(dead) != 1 || dead[0].Attempts != 2 || !strings.Contains(dead[0].LastError, "502") {
		t.Fatalf("dead = %+v %v", dead, err)
	}
	if strings.Contains(logs.String(), secret) {
		t.Fatalf("secret logged: %s", logs.String())
	}
	if metrics.Sample("bezorg_webhook_deliveries_total", map[string]string{"result": "retry"}) < 1 || metrics.Sample("bezorg_webhook_deliveries_total", map[string]string{"result": "dead"}) < 1 {
		t.Fatal("delivery metrics were not recorded")
	}
	reopened, err := Open(filepath.Dir(store.path))
	if err != nil {
		t.Fatal(err)
	}
	again, err := reopened.Deliveries(context.Background(), hooks[0].ID, "dead", 10)
	if err != nil || len(again) != 1 || again[0].DeliveryID != dead[0].DeliveryID {
		t.Fatalf("reopened = %+v %v", again, err)
	}
}

func TestReconcileRepairsAMissedDelivery(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), Params{URL: "https://agent.example/hooks", Secret: "super-secret-value", CreatedSeq: 1}); err != nil {
		t.Fatal(err)
	}
	batch := []events.Event{
		{Seq: 1, Type: events.DownloadQueued, JobID: "old", Status: "queued", CreatedAt: time.Unix(1, 0).UTC()},
		{Seq: 2, Type: events.DownloadSucceeded, JobID: "new", ExternalID: "batch-9", Status: "completed", CreatedAt: time.Unix(2, 0).UTC()},
	}
	if err := store.Reconcile(batch); err != nil {
		t.Fatal(err)
	}
	if err := store.Reconcile(batch); err != nil {
		t.Fatal(err)
	}
	hooks, _ := store.List(context.Background())
	got, err := store.Deliveries(context.Background(), hooks[0].ID, "", 10)
	if err != nil || len(got) != 1 || got[0].EventSeq != 2 || !strings.Contains(got[0].Payload, "batch-9") {
		t.Fatalf("deliveries = %+v %v", got, err)
	}
}
