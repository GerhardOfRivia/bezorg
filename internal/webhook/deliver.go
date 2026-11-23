package webhook

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/metrics"
)

const httpTimeout = 10 * time.Second

// Run posts due deliveries until ctx is canceled. A crash retries after the
// lease instead of losing the event. Redirects are not followed.
func Run(ctx context.Context, store *Store, logger *slog.Logger) {
	if store == nil {
		return
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	client := &http.Client{
		Timeout: httpTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := deliverDue(ctx, store, client, logger); err != nil && ctx.Err() == nil {
			logger.Warn("webhook delivery failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func deliverDue(ctx context.Context, store *Store, client *http.Client, logger *slog.Logger) error {
	attempts, err := store.Claim(8)
	if err != nil || len(attempts) == 0 {
		return err
	}
	var sendErr error
	for _, attempt := range attempts {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		status, cause := post(ctx, client, attempt)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		result, finishErr := store.Finish(attempt.ID, status >= 200 && status < 300, cause)
		if finishErr != nil {
			sendErr = errors.Join(sendErr, finishErr)
			continue
		}
		switch result {
		case "succeeded":
			metrics.WebhookDelivery("succeeded")
			logger.Debug("delivered webhook", "webhook_id", attempt.WebhookID, "delivery_id", attempt.DeliveryID, "event", attempt.EventType)
		case "dead":
			metrics.WebhookDelivery("dead")
			logger.Warn("webhook delivery dead-lettered", "webhook_id", attempt.WebhookID, "delivery_id", attempt.DeliveryID, "event", attempt.EventType, "error", cause)
		default:
			metrics.WebhookDelivery("retry")
			logger.Warn("webhook delivery will retry", "webhook_id", attempt.WebhookID, "delivery_id", attempt.DeliveryID, "event", attempt.EventType, "error", cause)
		}
	}
	return sendErr
}

func post(ctx context.Context, client *http.Client, attempt Attempt) (int, string) {
	body := []byte(attempt.Payload)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, attempt.URL, strings.NewReader(attempt.Payload))
	if err != nil {
		return 0, "invalid webhook url"
	}
	timestamp := time.Now().UTC()
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "bezorg-webhook")
	request.Header.Set("X-bezorg-Event", attempt.EventType)
	request.Header.Set("X-bezorg-Delivery", attempt.DeliveryID)
	request.Header.Set("X-bezorg-Signature", Sign(attempt.Secret, body, timestamp))
	response, err := client.Do(request)
	if err != nil {
		return 0, "request failed"
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response.StatusCode, fmt.Sprintf("webhook returned HTTP %d", response.StatusCode)
	}
	return response.StatusCode, ""
}
