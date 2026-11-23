package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/events"
)

func TestEventsAPILongPollSSEAndResume(t *testing.T) {
	svc := &fakeService{feed: []events.Event{
		{Seq: 1, Type: events.DownloadQueued, JobID: "job-1", ExternalID: "batch-42", Remote: "/drop/a.txt", Local: "/data/a.txt", Status: "queued", Bytes: 0, CreatedAt: time.Unix(1, 0).UTC()},
		{Seq: 2, Type: events.DownloadStarted, JobID: "job-1", ExternalID: "batch-42", Remote: "/drop/a.txt", Local: "/data/a.txt", Status: "running", Attempts: 0, Bytes: 1, CreatedAt: time.Unix(2, 0).UTC()},
		{Seq: 3, Type: events.DownloadSucceeded, JobID: "job-1", ExternalID: "batch-42", Remote: "/drop/a.txt", Local: "/data/a.txt", Status: "completed", Attempts: 0, Bytes: 5, CreatedAt: time.Unix(3, 0).UTC()},
	}}
	handler := handlerFor(t, svc, false)
	path := "/api/v1/events"

	poll := httptest.NewRequest(http.MethodGet, "http://127.0.0.1"+path+"?timeout=0&after=0&stream=poll", nil)
	poll.Header.Set("Authorization", "Bearer "+handler.token)
	poll.Header.Set("Accept", "text/event-stream")
	poll.Host = "127.0.0.1"
	recorded := httptest.NewRecorder()
	handler.ServeHTTP(recorded, poll)
	if recorded.Code != http.StatusOK || strings.Contains(recorded.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("poll status=%d type=%s body=%s", recorded.Code, recorded.Header().Get("Content-Type"), recorded.Body.String())
	}
	body := recorded.Body.String()
	if !strings.Contains(body, `"type": "download.queued"`) || !strings.Contains(body, `"external_id": "batch-42"`) || !strings.Contains(body, `"local": "/data/a.txt"`) || !strings.Contains(body, `"next": 3`) {
		t.Fatalf("poll body = %s", body)
	}

	sse := httptest.NewRequest(http.MethodGet, "http://127.0.0.1"+path+"?timeout=0", nil)
	sse.Header.Set("Authorization", "Bearer "+handler.token)
	sse.Header.Set("Accept", "text/event-stream")
	sse.Host = "127.0.0.1"
	recorded = httptest.NewRecorder()
	handler.ServeHTTP(recorded, sse)
	if recorded.Code != http.StatusOK || !strings.HasPrefix(recorded.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("sse status=%d type=%s", recorded.Code, recorded.Header().Get("Content-Type"))
	}
	if !strings.Contains(recorded.Body.String(), "id: 1\n") || !strings.Contains(recorded.Body.String(), "event: download.queued\n") || !strings.Contains(recorded.Body.String(), `"seq":1`) {
		t.Fatalf("sse body = %s", recorded.Body.String())
	}

	resume := httptest.NewRequest(http.MethodGet, "http://127.0.0.1"+path+"?timeout=0&after=0", nil)
	resume.Header.Set("Authorization", "Bearer "+handler.token)
	resume.Header.Set("Last-Event-ID", "2")
	resume.Host = "127.0.0.1"
	recorded = httptest.NewRecorder()
	handler.ServeHTTP(recorded, resume)
	if !strings.Contains(recorded.Body.String(), `"seq": 3`) || strings.Contains(recorded.Body.String(), `"seq": 1`) {
		t.Fatalf("resume = %s", recorded.Body.String())
	}

	for _, target := range []string{path + "?stream=neither", path + "?after=-1", path + "?timeout=61", path + "?timeout=soon"} {
		request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1"+target, nil)
		request.Header.Set("Authorization", "Bearer "+handler.token)
		request.Host = "127.0.0.1"
		recorded = httptest.NewRecorder()
		handler.ServeHTTP(recorded, request)
		if recorded.Code != http.StatusBadRequest {
			t.Fatalf("%s status=%d body=%s", target, recorded.Code, recorded.Body.String())
		}
	}
}

func TestEventsAPIWaitsForTheNextEvent(t *testing.T) {
	svc := &fakeService{feed: []events.Event{{Seq: 1, Type: events.DownloadQueued, JobID: "job-1", Status: "queued", CreatedAt: time.Unix(1, 0).UTC()}}}
	handler := handlerFor(t, svc, false)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/api/v1/events?stream=sse&timeout=2&after=1", nil)
		request.Header.Set("Authorization", "Bearer "+handler.token)
		request.Host = "127.0.0.1"
		recorded := httptest.NewRecorder()
		handler.ServeHTTP(recorded, request)
		done <- recorded
	}()
	time.Sleep(100 * time.Millisecond)
	svc.mu.Lock()
	svc.feed = append(svc.feed, events.Event{Seq: 2, Type: events.DownloadSucceeded, JobID: "job-1", ExternalID: "later-1", Remote: "/drop/later.txt", Local: "/data/later.txt", Status: "completed", Bytes: 8, CreatedAt: time.Unix(2, 0).UTC()})
	svc.mu.Unlock()
	select {
	case recorded := <-done:
		if recorded.Code != http.StatusOK || !strings.Contains(recorded.Body.String(), "event: download.succeeded") || !strings.Contains(recorded.Body.String(), "later-1") {
			t.Fatalf("waited sse = %d %s", recorded.Code, recorded.Body.String())
		}
	case <-time.After(4 * time.Second):
		t.Fatal("event stream did not return")
	}
}

func TestPreferEventStream(t *testing.T) {
	if preferEventStream("") || preferEventStream("*/*") || preferEventStream("application/json") {
		t.Fatal("json clients were treated as SSE")
	}
	if !preferEventStream("text/event-stream") || !preferEventStream("text/event-stream, application/json;q=0.9") {
		t.Fatal("event-stream was not preferred")
	}
	if preferEventStream("application/json, text/event-stream;q=0.1") {
		t.Fatal("lower-q event-stream won")
	}
}

type tokenHandler struct {
	http.Handler
	token string
}

func handlerFor(t *testing.T, svc *fakeService, public bool) tokenHandler {
	t.Helper()
	server := startServer(t, svc, "127.0.0.1:0", public)
	return tokenHandler{Handler: server.httpServer.Handler, token: readToken(t, server)}
}
