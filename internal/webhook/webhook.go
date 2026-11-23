// Package webhook stores subscriptions and delivers download events with
// HMAC signatures. Secrets stay in the state file and in memory used for
// signing. They are not returned to API callers and must not be logged.
package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/events"
)

const (
	// DefaultAttempts is the number of HTTP tries before a delivery is
	// dead-lettered. A crash while a try is leased does not consume one.
	DefaultAttempts = 8
	// MaxAttempts is the largest per-webhook try budget.
	MaxAttempts = 20
	// MaxHooks bounds the subscriptions consulted on every event.
	MaxHooks = 20

	leaseDuration = 30 * time.Second
	backoffCap    = 5 * time.Minute
	minSecret     = 16
	maxSecret     = 256
	maxURL        = 2048
)

var (
	// ErrNotFound means the webhook id is not stored.
	ErrNotFound = errors.New("webhook not found")
	// ErrLimit means the daemon already has MaxHooks subscriptions.
	ErrLimit = errors.New("webhook limit reached")
)

// Hook is a subscription. Secret is never included.
type Hook struct {
	ID          int64     `json:"id"`
	URL         string    `json:"url"`
	Events      []string  `json:"events"`
	MaxAttempts int       `json:"max_attempts"`
	CreatedAt   time.Time `json:"created_at"`
}

// Params is a new subscription. An empty Events list matches every event type.
type Params struct {
	URL         string
	Secret      string
	Events      []string
	MaxAttempts int
	// CreatedSeq is the event high-water at creation. Earlier events are not delivered.
	CreatedSeq int64
}

// Delivery is one durable attempt record without the signing secret.
type Delivery struct {
	ID            int64     `json:"id"`
	WebhookID     int64     `json:"webhook_id"`
	EventSeq      int64     `json:"event_seq"`
	DeliveryID    string    `json:"delivery_id"`
	EventType     string    `json:"event_type"`
	Payload       string    `json:"payload"`
	Status        string    `json:"status"`
	Attempts      int       `json:"attempts"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	LastError     string    `json:"last_error,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Attempt is a leased delivery plus the material needed to send it.
// Secret must not be logged.
type Attempt struct {
	ID          int64
	WebhookID   int64
	EventSeq    int64
	DeliveryID  string
	EventType   string
	Payload     string
	Attempts    int
	MaxAttempts int
	URL         string
	Secret      string
}

type storedHook struct {
	ID          int64     `json:"id"`
	URL         string    `json:"url"`
	Secret      string    `json:"secret"`
	Events      []string  `json:"events"`
	MaxAttempts int       `json:"max_attempts"`
	CreatedAt   time.Time `json:"created_at"`
	CreatedSeq  int64     `json:"created_seq"`
}

type fileState struct {
	NextHookID     int64        `json:"next_hook_id"`
	NextDeliveryID int64        `json:"next_delivery_id"`
	Hooks          []storedHook `json:"hooks"`
	Rows           []Delivery   `json:"deliveries"`
}

// Store is safe for concurrent use. State is rewritten atomically.
type Store struct {
	mu   sync.Mutex
	path string
	fileState
}

// Open loads subscriptions from dir/webhooks.json. The file is mode 0600.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	store := &Store{path: filepath.Join(dir, "webhooks.json")}
	store.NextHookID = 1
	store.NextDeliveryID = 1
	data, err := os.ReadFile(store.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return store, nil
		}
		return nil, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return store, nil
	}
	if err := json.Unmarshal(data, &store.fileState); err != nil {
		return nil, fmt.Errorf("webhook: decode %s: %w", store.path, err)
	}
	if store.NextHookID < 1 {
		store.NextHookID = 1
	}
	if store.NextDeliveryID < 1 {
		store.NextDeliveryID = 1
	}
	for i := range store.Hooks {
		if store.Hooks[i].Events == nil {
			store.Hooks[i].Events = []string{}
		}
	}
	if store.Rows == nil {
		store.Rows = []Delivery{}
	}
	return store, nil
}

// Sign returns the X-bezorg-Signature value for body. The signed string is
// the decimal unix timestamp, a period, and the exact request body bytes.
// The header is "t=<unix>,v1=<hex>" with a lowercase hex HMAC-SHA256.
func Sign(secret string, body []byte, timestamp time.Time) string {
	unix := timestamp.Unix()
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = fmt.Fprintf(mac, "%d.", unix)
	_, _ = mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", unix, hex.EncodeToString(mac.Sum(nil)))
}

// Create stores a subscription. The secret is kept for signing and is not returned.
func (s *Store) Create(_ context.Context, params Params) (Hook, error) {
	normalized, err := normalize(params)
	if err != nil {
		return Hook{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.Hooks) >= MaxHooks {
		return Hook{}, ErrLimit
	}
	prev := s.snapshot()
	now := time.Now().UTC()
	id := s.NextHookID
	s.NextHookID++
	s.Hooks = append(s.Hooks, storedHook{
		ID: id, URL: normalized.URL, Secret: normalized.Secret, Events: normalized.Events,
		MaxAttempts: normalized.MaxAttempts, CreatedAt: now, CreatedSeq: normalized.CreatedSeq,
	})
	if err := s.persistLocked(); err != nil {
		s.restore(prev)
		return Hook{}, err
	}
	return Hook{ID: id, URL: normalized.URL, Events: append([]string(nil), normalized.Events...), MaxAttempts: normalized.MaxAttempts, CreatedAt: now}, nil
}

// List returns subscriptions in creation order. Secrets are not loaded into the result.
func (s *Store) List(context.Context) ([]Hook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Hook, 0, len(s.Hooks))
	for _, hook := range s.Hooks {
		events := append([]string(nil), hook.Events...)
		if events == nil {
			events = []string{}
		}
		out = append(out, Hook{ID: hook.ID, URL: hook.URL, Events: events, MaxAttempts: hook.MaxAttempts, CreatedAt: hook.CreatedAt})
	}
	return out, nil
}

// Delete removes a subscription and its delivery history.
func (s *Store) Delete(_ context.Context, id int64) error {
	if id <= 0 {
		return ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.snapshot()
	kept := make([]storedHook, 0, len(s.Hooks))
	found := false
	for _, hook := range s.Hooks {
		if hook.ID == id {
			found = true
			continue
		}
		kept = append(kept, hook)
	}
	if !found {
		return ErrNotFound
	}
	filtered := make([]Delivery, 0, len(s.Rows))
	for _, delivery := range s.Rows {
		if delivery.WebhookID != id {
			filtered = append(filtered, delivery)
		}
	}
	s.Hooks = kept
	s.Rows = filtered
	if err := s.persistLocked(); err != nil {
		s.restore(prev)
		return err
	}
	return nil
}

// Deliveries lists records for one webhook, oldest first.
// status filters pending, succeeded, or dead. An empty status returns all.
func (s *Store) Deliveries(_ context.Context, webhookID int64, status string, limit int) ([]Delivery, error) {
	if webhookID <= 0 {
		return nil, ErrNotFound
	}
	switch status {
	case "", "pending", "succeeded", "dead":
	default:
		return nil, fmt.Errorf("webhook: invalid delivery status %q", status)
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 200 {
		limit = 200
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasHook(webhookID) {
		return nil, ErrNotFound
	}
	out := make([]Delivery, 0)
	for _, delivery := range s.Rows {
		if delivery.WebhookID != webhookID {
			continue
		}
		if status != "" && delivery.Status != status {
			continue
		}
		copied := delivery
		out = append(out, copied)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// Enqueue records a pending delivery for every matching subscription.
// Events at or before the subscription's created sequence are ignored.
func (s *Store) Enqueue(event events.Event) error {
	if event.Seq <= 0 {
		return errors.New("webhook: event seq is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.snapshot()
	added := false
	for _, hook := range s.Hooks {
		if event.Seq <= hook.CreatedSeq || !hookWants(hook.Events, event.Type) || s.hasPair(hook.ID, event.Seq) {
			continue
		}
		if err := s.appendDeliveryLocked(hook, event); err != nil {
			s.restore(prev)
			return err
		}
		added = true
	}
	if !added {
		return nil
	}
	if err := s.persistLocked(); err != nil {
		s.restore(prev)
		return err
	}
	return nil
}

// Reconcile creates any delivery that an event should have produced. A crash
// between the event append and this write is repaired on the next start.
func (s *Store) Reconcile(batch []events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.snapshot()
	added := false
	for _, event := range batch {
		for _, hook := range s.Hooks {
			if event.Seq <= hook.CreatedSeq || !hookWants(hook.Events, event.Type) || s.hasPair(hook.ID, event.Seq) {
				continue
			}
			if err := s.appendDeliveryLocked(hook, event); err != nil {
				s.restore(prev)
				return err
			}
			added = true
		}
	}
	if !added {
		return nil
	}
	if err := s.persistLocked(); err != nil {
		s.restore(prev)
		return err
	}
	return nil
}

// Claim leases due pending deliveries. The lease does not count as an attempt.
func (s *Store) Claim(limit int) ([]Attempt, error) {
	if limit <= 0 {
		limit = 8
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.snapshot()
	now := time.Now().UTC()
	attempts := make([]Attempt, 0)
	for i := range s.Rows {
		delivery := &s.Rows[i]
		if delivery.Status != "pending" || delivery.NextAttemptAt.After(now) {
			continue
		}
		hook, ok := s.hook(delivery.WebhookID)
		if !ok {
			continue
		}
		attempts = append(attempts, Attempt{
			ID: delivery.ID, WebhookID: delivery.WebhookID, EventSeq: delivery.EventSeq,
			DeliveryID: delivery.DeliveryID, EventType: delivery.EventType, Payload: delivery.Payload,
			Attempts: delivery.Attempts, MaxAttempts: hook.MaxAttempts, URL: hook.URL, Secret: hook.Secret,
		})
		delivery.NextAttemptAt = now.Add(leaseDuration)
		delivery.UpdatedAt = now
		if len(attempts) == limit {
			break
		}
	}
	if len(attempts) == 0 {
		return nil, nil
	}
	if err := s.persistLocked(); err != nil {
		s.restore(prev)
		return nil, err
	}
	return attempts, nil
}

// Finish records one leased try. A failure uses exponential backoff capped at
// five minutes and dead-letters the row once max attempts is reached.
// The returned status is succeeded, pending, or dead.
func (s *Store) Finish(id int64, success bool, cause string) (string, error) {
	if id <= 0 {
		return "", ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.snapshot()
	delivery, hook, err := s.pending(id)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	tries := delivery.Attempts + 1
	status := "succeeded"
	next := now
	lastError := ""
	if !success {
		lastError = truncateError(cause)
		if tries >= hook.MaxAttempts {
			status = "dead"
		} else {
			status = "pending"
			next = now.Add(backoffFor(tries))
		}
	}
	delivery.Status = status
	delivery.Attempts = tries
	delivery.NextAttemptAt = next
	delivery.LastError = lastError
	delivery.UpdatedAt = now
	if err := s.persistLocked(); err != nil {
		s.restore(prev)
		return "", err
	}
	return status, nil
}

func (s *Store) snapshot() fileState {
	return fileState{
		NextHookID:     s.NextHookID,
		NextDeliveryID: s.NextDeliveryID,
		Hooks:          append([]storedHook(nil), s.Hooks...),
		Rows:           append([]Delivery(nil), s.Rows...),
	}
}

func (s *Store) restore(prev fileState) {
	s.fileState = prev
}

func (s *Store) appendDeliveryLocked(hook storedHook, event events.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	deliveryID, err := newDeliveryID()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	s.Rows = append(s.Rows, Delivery{
		ID: s.NextDeliveryID, WebhookID: hook.ID, EventSeq: event.Seq, DeliveryID: deliveryID,
		EventType: event.Type, Payload: string(payload), Status: "pending", Attempts: 0,
		NextAttemptAt: now, CreatedAt: now, UpdatedAt: now,
	})
	s.NextDeliveryID++
	return nil
}

func (s *Store) pending(id int64) (*Delivery, storedHook, error) {
	for i := range s.Rows {
		if s.Rows[i].ID != id {
			continue
		}
		if s.Rows[i].Status != "pending" {
			return nil, storedHook{}, ErrNotFound
		}
		hook, ok := s.hook(s.Rows[i].WebhookID)
		if !ok {
			return nil, storedHook{}, ErrNotFound
		}
		return &s.Rows[i], hook, nil
	}
	return nil, storedHook{}, ErrNotFound
}

func (s *Store) hook(id int64) (storedHook, bool) {
	for _, hook := range s.Hooks {
		if hook.ID == id {
			return hook, true
		}
	}
	return storedHook{}, false
}

func (s *Store) hasHook(id int64) bool {
	_, ok := s.hook(id)
	return ok
}

func (s *Store) hasPair(webhookID, seq int64) bool {
	for _, delivery := range s.Rows {
		if delivery.WebhookID == webhookID && delivery.EventSeq == seq {
			return true
		}
	}
	return false
}

func (s *Store) persistLocked() error {
	if s.Hooks == nil {
		s.Hooks = []storedHook{}
	}
	if s.Rows == nil {
		s.Rows = []Delivery{}
	}
	data, err := json.MarshalIndent(s.fileState, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeAtomic(s.path, data, 0o600)
}

func normalize(params Params) (Params, error) {
	rawURL := strings.TrimSpace(params.URL)
	if rawURL == "" || len(rawURL) > maxURL || strings.ContainsRune(rawURL, 0) {
		return Params{}, errors.New("webhook: url must be an http(s) URL of at most 2048 bytes")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") {
		return Params{}, errors.New("webhook: url must be http or https, with a host and without user info or a fragment")
	}
	secret := params.Secret
	if strings.ContainsRune(secret, 0) || len(secret) < minSecret || len(secret) > maxSecret {
		return Params{}, fmt.Errorf("webhook: secret must be %d to %d bytes and cannot contain NUL", minSecret, maxSecret)
	}
	names := make([]string, 0, len(params.Events))
	seen := map[string]bool{}
	for _, event := range params.Events {
		event = strings.TrimSpace(event)
		if event == "" {
			continue
		}
		if !validEvent(event) {
			return Params{}, fmt.Errorf("webhook: unknown event %q", event)
		}
		if seen[event] {
			return Params{}, fmt.Errorf("webhook: duplicate event %q", event)
		}
		seen[event] = true
		names = append(names, event)
	}
	attempts := params.MaxAttempts
	if attempts == 0 {
		attempts = DefaultAttempts
	}
	if attempts < 1 || attempts > MaxAttempts {
		return Params{}, fmt.Errorf("webhook: max attempts must be from 1 to %d", MaxAttempts)
	}
	if params.CreatedSeq < 0 {
		return Params{}, errors.New("webhook: created sequence cannot be negative")
	}
	params.URL = rawURL
	params.Events = names
	params.MaxAttempts = attempts
	return params, nil
}

func validEvent(event string) bool {
	switch event {
	case events.DownloadQueued, events.DownloadStarted, events.DownloadSucceeded,
		events.DownloadFailed, events.DownloadRetried, events.DownloadInterrupted:
		return true
	default:
		return false
	}
}

func hookWants(subscribed []string, eventType string) bool {
	if len(subscribed) == 0 {
		return true
	}
	for _, event := range subscribed {
		if event == eventType {
			return true
		}
	}
	return false
}

func webhookBackoff(tries int) time.Duration {
	if tries < 1 {
		tries = 1
	}
	if tries > 8 {
		tries = 8
	}
	delay := time.Second << tries
	if delay <= 0 || delay > backoffCap {
		return backoffCap
	}
	return delay
}

// backoffFor is replaced in tests so a retry does not wait two seconds.
var backoffFor = webhookBackoff

func truncateError(cause string) string {
	cause = strings.TrimSpace(strings.ReplaceAll(cause, "\n", " "))
	if len(cause) > 300 {
		cause = cause[:300]
	}
	return cause
}

func newDeliveryID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("webhook: delivery id: %w", err)
	}
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16]), nil
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".webhook-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	ok = true
	return nil
}
