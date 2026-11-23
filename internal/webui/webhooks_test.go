package webui

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/GerhardOfRivia/bezorg/internal/events"
	"github.com/GerhardOfRivia/bezorg/internal/webhook"
)

func TestWebhookAPIHidesTheSecret(t *testing.T) {
	store, err := webhook.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc := &fakeService{hooks: store}
	handler := handlerFor(t, svc, false)
	const secret = "super-secret-value"
	body := `{"url":"https://agent.example/hooks","secret":"` + secret + `","events":["download.succeeded"],"max_attempts":8}`
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/v1/webhooks", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+handler.token)
	request.Header.Set("Content-Type", "application/json")
	request.Host = "127.0.0.1"
	recorded := httptest.NewRecorder()
	handler.ServeHTTP(recorded, request)
	if recorded.Code != http.StatusCreated || strings.Contains(recorded.Body.String(), secret) || !strings.Contains(recorded.Body.String(), "https://agent.example/hooks") {
		t.Fatalf("create %d %s", recorded.Code, recorded.Body.String())
	}
	var created webhook.Hook
	if err := json.Unmarshal(recorded.Body.Bytes(), &created); err != nil || created.ID == 0 {
		t.Fatal(err)
	}
	if err := store.Enqueue(events.Event{Seq: 3, Type: events.DownloadSucceeded, JobID: "job-9", ExternalID: "batch-9", Remote: "/drop/a.txt", Local: "/data/a.txt", Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	list := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/api/v1/webhooks/"+itoa(created.ID)+"/deliveries", nil)
	list.Header.Set("Authorization", "Bearer "+handler.token)
	list.Host = "127.0.0.1"
	recorded = httptest.NewRecorder()
	handler.ServeHTTP(recorded, list)
	if recorded.Code != http.StatusOK || !strings.Contains(recorded.Body.String(), "batch-9") || strings.Contains(recorded.Body.String(), secret) {
		t.Fatalf("deliveries %d %s", recorded.Code, recorded.Body.String())
	}
	anonymous := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/v1/webhooks", bytes.NewReader([]byte(body)))
	anonymous.Host = "127.0.0.1"
	recorded = httptest.NewRecorder()
	handler.ServeHTTP(recorded, anonymous)
	if recorded.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous create %d", recorded.Code)
	}
}

func itoa(id int64) string {
	return strconv.FormatInt(id, 10)
}
