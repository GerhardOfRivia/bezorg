package webui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/events"
)

const (
	defaultEventTimeout = 25 * time.Second
	maximumEventTimeout = 60 * time.Second
	eventPollInterval   = 200 * time.Millisecond
	maxEventsPerRead    = 100
)

type eventsResponse struct {
	Events []events.Event `json:"events"`
	After  int64          `json:"after"`
	Next   int64          `json:"next"`
}

func (api apiServer) events(output http.ResponseWriter, request *http.Request) {
	mode, err := eventMode(request)
	if err != nil {
		writeAPIError(output, http.StatusBadRequest, err.Error())
		return
	}
	cursor, err := eventCursor(request)
	if err != nil {
		writeAPIError(output, http.StatusBadRequest, err.Error())
		return
	}
	timeout, err := eventTimeout(request.URL.Query())
	if err != nil {
		writeAPIError(output, http.StatusBadRequest, err.Error())
		return
	}
	// HEAD must not wait. Public-read checks and browsers use it as a probe.
	if request.Method == http.MethodHead {
		mode = "poll"
		timeout = 0
	}
	if mode == "sse" {
		api.streamEvents(output, request, cursor, timeout)
		return
	}
	batch, err := waitForEvents(request.Context(), api.svc, cursor, timeout)
	if err != nil {
		if request.Context().Err() != nil {
			return
		}
		writeAPIError(output, http.StatusServiceUnavailable, err.Error())
		return
	}
	if batch == nil {
		batch = []events.Event{}
	}
	writeJSON(output, http.StatusOK, eventsResponse{Events: batch, After: cursor, Next: nextEventCursor(cursor, batch)})
}

func (api apiServer) streamEvents(output http.ResponseWriter, request *http.Request, cursor int64, timeout time.Duration) {
	batch, err := api.svc.Events(request.Context(), cursor, maxEventsPerRead)
	if err != nil {
		if request.Context().Err() == nil {
			writeAPIError(output, http.StatusServiceUnavailable, err.Error())
		}
		return
	}
	controller := http.NewResponseController(output)
	extend := func() {
		_ = controller.SetWriteDeadline(time.Now().Add(15 * time.Second))
	}
	extend()
	output.Header().Set("Content-Type", "text/event-stream")
	output.Header().Set("Cache-Control", "no-store")
	output.Header().Set("X-Accel-Buffering", "no")
	flush := func() {
		if err := controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return
		}
	}
	writeBatch := func(eventsBatch []events.Event) bool {
		for _, event := range eventsBatch {
			extend()
			if err := writeSSE(output, event); err != nil {
				return false
			}
			cursor = event.Seq
		}
		if len(eventsBatch) > 0 {
			flush()
		}
		return true
	}
	if !writeBatch(batch) {
		return
	}
	deadline := time.Now().Add(timeout)
	nextKeepalive := time.Now().Add(15 * time.Second)
	for timeout > 0 && time.Now().Before(deadline) {
		if len(batch) < maxEventsPerRead {
			wait := eventPollInterval
			if remaining := time.Until(deadline); remaining < wait {
				wait = remaining
			}
			if wait <= 0 {
				return
			}
			timer := time.NewTimer(wait)
			select {
			case <-request.Context().Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		if err := request.Context().Err(); err != nil {
			return
		}
		batch, err = api.svc.Events(request.Context(), cursor, maxEventsPerRead)
		if err != nil {
			if request.Context().Err() == nil {
				fmt.Fprintf(output, ": error %s\n\n", strings.ReplaceAll(err.Error(), "\n", " "))
				flush()
			}
			return
		}
		if len(batch) == 0 {
			if time.Now().Before(nextKeepalive) {
				continue
			}
			extend()
			if _, err := output.Write([]byte(": keepalive\n\n")); err != nil {
				return
			}
			flush()
			nextKeepalive = time.Now().Add(15 * time.Second)
			continue
		}
		if !writeBatch(batch) {
			return
		}
	}
}

func waitForEvents(ctx context.Context, svc interface {
	Events(context.Context, int64, int) ([]events.Event, error)
}, after int64, timeout time.Duration) ([]events.Event, error) {
	deadline := time.Now().Add(timeout)
	for {
		batch, err := svc.Events(ctx, after, maxEventsPerRead)
		if err != nil || len(batch) > 0 || timeout == 0 || !time.Now().Before(deadline) {
			return batch, err
		}
		wait := eventPollInterval
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		if wait <= 0 {
			return batch, nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func writeSSE(output http.ResponseWriter, event events.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, "id: %d\nevent: %s\ndata: %s\n\n", event.Seq, event.Type, payload)
	return err
}

func nextEventCursor(after int64, batch []events.Event) int64 {
	if len(batch) == 0 {
		return after
	}
	return batch[len(batch)-1].Seq
}

func eventMode(request *http.Request) (string, error) {
	switch strings.ToLower(strings.TrimSpace(request.URL.Query().Get("stream"))) {
	case "":
		if preferEventStream(request.Header.Get("Accept")) {
			return "sse", nil
		}
		return "poll", nil
	case "sse":
		return "sse", nil
	case "poll":
		return "poll", nil
	default:
		return "", errors.New("stream must be sse or poll")
	}
}

// preferEventStream reports whether Accept prefers text/event-stream over JSON.
// An empty Accept or */* stays on the JSON long-poll so plain curl clients do
// not have to set a header. An explicit stream query parameter wins over Accept.
func preferEventStream(header string) bool {
	bestJSON := -1.0
	bestSSE := -1.0
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		media, params, err := mime.ParseMediaType(part)
		if err != nil {
			continue
		}
		quality := 1.0
		if raw := params["q"]; raw != "" {
			parsed, parseErr := strconv.ParseFloat(raw, 64)
			if parseErr != nil || parsed < 0 {
				continue
			}
			quality = parsed
		}
		switch media {
		case "text/event-stream":
			if quality > bestSSE {
				bestSSE = quality
			}
		case "application/json", "*/*":
			if quality > bestJSON {
				bestJSON = quality
			}
		}
	}
	return bestSSE >= 0 && bestSSE >= bestJSON
}

func eventCursor(request *http.Request) (int64, error) {
	raw := strings.TrimSpace(request.Header.Get("Last-Event-ID"))
	name := "Last-Event-ID"
	if raw == "" {
		raw = strings.TrimSpace(request.URL.Query().Get("after"))
		name = "after"
	}
	if raw == "" {
		return 0, nil
	}
	cursor, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || cursor < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return cursor, nil
}

func eventTimeout(values interface{ Get(string) string }) (time.Duration, error) {
	raw := strings.TrimSpace(values.Get("timeout"))
	if raw == "" {
		return defaultEventTimeout, nil
	}
	seconds, err := strconv.Atoi(raw)
	maximum := int(maximumEventTimeout / time.Second)
	if err != nil || seconds < 0 || seconds > maximum {
		return 0, fmt.Errorf("timeout must be an integer number of seconds from 0 to %d", maximum)
	}
	return time.Duration(seconds) * time.Second, nil
}
