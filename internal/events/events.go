// Package events is the durable, monotonic record of download transitions.
// Sequence numbers survive restarts and are not reused when old events are
// dropped. The log is an append-only JSONL file plus a high-water seq file,
// the same atomic-rename style as job records.
package events

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	DownloadQueued      = "download.queued"
	DownloadStarted     = "download.started"
	DownloadSucceeded   = "download.succeeded"
	DownloadFailed      = "download.failed"
	DownloadRetried     = "download.retried"
	DownloadInterrupted = "download.interrupted"

	// DefaultRetention is the number of newest events kept. Older lines are
	// deleted. Sequence numbers already issued are not reused.
	DefaultRetention = 10000
)

// Event is one durable download transition. Seq is monotonic for this state
// directory. CreatedAt is when the event was recorded.
type Event struct {
	Seq          int64      `json:"seq"`
	Type         string     `json:"type"`
	JobID        string     `json:"job_id"`
	ExternalID   string     `json:"external_id,omitempty"`
	Remote       string     `json:"remote"`
	Local        string     `json:"local"`
	Status       string     `json:"status"`
	Attempts     int        `json:"attempts"`
	Bytes        int64      `json:"bytes"`
	CreatedAt    time.Time  `json:"created_at"`
	JobCreatedAt time.Time  `json:"job_created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	// Manifest is the absolute path of the completion marker. It is set on
	// download.succeeded, after that file has been renamed into place.
	Manifest string `json:"manifest,omitempty"`
}

// Log is safe for concurrent append and read.
type Log struct {
	mu     sync.Mutex
	dir    string
	logger *slog.Logger
	next   int64
	keep   int
	events []Event
}

// Open loads the log under dir. A torn final line, from a crash during append,
// is ignored. The next sequence is one past the higher of the seq file and the
// last complete line, so a sequence is never reused.
func Open(dir string, logger *slog.Logger) (*Log, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	log := &Log{dir: dir, logger: logger, next: 1, keep: DefaultRetention}
	high, err := readSeq(filepath.Join(dir, "seq"))
	if err != nil {
		return nil, err
	}
	events, err := readLog(filepath.Join(dir, "log.jsonl"), logger)
	if err != nil {
		return nil, err
	}
	var maxSeq int64
	for _, event := range events {
		if event.Seq > maxSeq {
			maxSeq = event.Seq
		}
	}
	if high > maxSeq {
		maxSeq = high
	}
	log.next = maxSeq + 1
	log.events = events
	if len(log.events) > log.keep {
		if err := log.pruneLocked(); err != nil {
			return nil, err
		}
	}
	return log, nil
}

// Append assigns the next sequence, syncs the line, then advances the seq
// file. CreatedAt is set when it is zero.
func (l *Log) Append(event Event) (Event, error) {
	if event.Type == "" || event.JobID == "" {
		return Event{}, errors.New("events: type and job id are required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	event.Seq = l.next
	line, err := json.Marshal(event)
	if err != nil {
		return Event{}, err
	}
	line = append(line, '\n')
	file, err := os.OpenFile(l.path(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return Event{}, err
	}
	if _, err := file.Write(line); err != nil {
		_ = file.Close()
		return Event{}, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return Event{}, err
	}
	if err := file.Close(); err != nil {
		return Event{}, err
	}
	if err := writeSeq(filepath.Join(l.dir, "seq"), event.Seq); err != nil {
		return Event{}, err
	}
	l.next++
	l.events = append(l.events, event)
	if l.keep > 0 && len(l.events) > l.keep && event.Seq%1000 == 0 {
		if err := l.pruneLocked(); err != nil {
			return Event{}, err
		}
	}
	return event, nil
}

// List returns events with seq greater than after, oldest first.
// limit <= 0 selects 100. limit above 1000 is capped.
func (l *Log) List(after int64, limit int) ([]Event, error) {
	if after < 0 {
		return nil, errors.New("events: cursor cannot be negative")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, 0)
	for _, event := range l.events {
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

func (l *Log) path() string {
	return filepath.Join(l.dir, "log.jsonl")
}

func (l *Log) pruneLocked() error {
	keep := l.keep
	if keep < 1 {
		keep = DefaultRetention
	}
	if len(l.events) <= keep {
		return nil
	}
	l.events = append([]Event(nil), l.events[len(l.events)-keep:]...)
	var buf bytes.Buffer
	for _, event := range l.events {
		line, err := json.Marshal(event)
		if err != nil {
			return err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return writeAtomic(l.path(), buf.Bytes(), 0o600)
}

func readLog(path string, logger *slog.Logger) ([]Event, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	events := make([]Event, 0)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 && (len(line) == 0 || line[len(line)-1] != '\n') && errors.Is(err, io.EOF) {
			logger.Warn("ignoring torn event line", "path", path)
			break
		}
		if len(bytes.TrimSpace(line)) > 0 {
			var event Event
			if unmarshalErr := json.Unmarshal(bytes.TrimSpace(line), &event); unmarshalErr != nil {
				return nil, fmt.Errorf("events: decode %s: %w", path, unmarshalErr)
			}
			if event.Seq > 0 {
				events = append(events, event)
			}
		}
		if errors.Is(err, io.EOF) {
			return events, nil
		}
		if err != nil {
			return nil, err
		}
	}
	return events, nil
}

func readSeq(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	text := string(bytes.TrimSpace(data))
	if text == "" {
		return 0, nil
	}
	var seq int64
	if _, err := fmt.Sscan(text, &seq); err != nil || seq < 0 {
		return 0, fmt.Errorf("events: seq file %s is invalid", path)
	}
	return seq, nil
}

func writeSeq(path string, seq int64) error {
	return writeAtomic(path, []byte(fmt.Sprintf("%d\n", seq)), 0o600)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".events-*.tmp")
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
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil
	}
	_ = dir.Sync()
	_ = dir.Close()
	return nil
}
