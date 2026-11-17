// Package job persists download jobs so a daemon restart can resume them.
package job

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/transfer"
)

const (
	StateQueued      = "queued"
	StateRunning     = "running"
	StateCompleted   = "completed"
	StateFailed      = "failed"
	StateCanceled    = "canceled"
	StateInterrupted = "interrupted"
)

var (
	// ErrNotFound means the selector matched nothing.
	ErrNotFound = errors.New("job not found")
	// ErrAmbiguous means a prefix matched more than one job.
	ErrAmbiguous = errors.New("ambiguous job selector")
)

// Record is the durable view of one download.
type Record struct {
	ID         string            `json:"id"`
	State      string            `json:"state"`
	CreatedAt  time.Time         `json:"created_at"`
	StartedAt  *time.Time        `json:"started_at,omitempty"`
	FinishedAt *time.Time        `json:"finished_at,omitempty"`
	UpdatedAt  time.Time         `json:"updated_at"`
	Attempt    int               `json:"attempt"`
	Config     config.Config     `json:"config"`
	Progress   transfer.Snapshot `json:"progress"`
	Error      string            `json:"error,omitempty"`
	ExitCode   int               `json:"exit_code"`
}

// Terminal reports whether the daemon will not touch the job again until
// an explicit retry.
func Terminal(state string) bool {
	switch state {
	case StateCompleted, StateFailed, StateCanceled, StateInterrupted:
		return true
	default:
		return false
	}
}

// Store is a directory of JSON job records. It is safe for concurrent use.
type Store struct {
	dir string
	log *slog.Logger
	mu  sync.Mutex
	all map[string]*Record
}

// Open loads every job under dir/jobs. A corrupt record is skipped.
func Open(dir string, logger *slog.Logger) (*Store, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	root := filepath.Join(dir, "jobs")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	store := &Store{dir: dir, log: logger, all: map[string]*Record{}}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(root, entry.Name(), "job.json")
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		var rec Record
		if err := json.Unmarshal(data, &rec); err != nil {
			logger.Error("skipping corrupt job record", "path", path, "error", err)
			continue
		}
		if rec.ID == "" {
			rec.ID = entry.Name()
		}
		store.all[rec.ID] = &rec
	}
	return store, nil
}

// Create queues a new job and returns a copy of its record.
func (s *Store) Create(cfg config.Config) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, err := newID()
	if err != nil {
		return Record{}, err
	}
	for {
		if _, ok := s.all[id]; !ok {
			break
		}
		id, err = newID()
		if err != nil {
			return Record{}, err
		}
	}
	now := time.Now().UTC()
	rec := &Record{
		ID:        id,
		State:     StateQueued,
		CreatedAt: now,
		UpdatedAt: now,
		Config:    cfg,
		ExitCode:  -1,
	}
	if err := s.save(rec); err != nil {
		return Record{}, err
	}
	s.all[id] = rec
	return *rec, nil
}

// Update mutates one job and writes it atomically.
func (s *Store) Update(id string, fn func(*Record)) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.exact(id)
	if err != nil {
		return Record{}, err
	}
	fn(rec)
	rec.UpdatedAt = time.Now().UTC()
	if err := s.save(rec); err != nil {
		return Record{}, err
	}
	return *rec, nil
}

// Get resolves an exact id or a unique prefix.
func (s *Store) Get(selector string) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.resolve(selector)
	if err != nil {
		return Record{}, err
	}
	return *rec, nil
}

// List returns jobs newest first.
func (s *Store) List() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, 0, len(s.all))
	for _, rec := range s.all {
		out = append(out, *rec)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out
}

// LogPath is the text log for a job.
func (s *Store) LogPath(id string) string {
	return filepath.Join(s.dir, "jobs", id, "log")
}

// AppendLog writes one already-formatted line.
func (s *Store) AppendLog(id, line string) error {
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	path := s.LogPath(id)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(line)
	return err
}

// Logs returns log lines starting at after, and the index to pass next time.
func (s *Store) Logs(id string, after, limit int) ([]string, int, error) {
	if _, err := s.Get(id); err != nil {
		return nil, after, err
	}
	data, err := os.ReadFile(s.LogPath(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, after, nil
		}
		return nil, after, err
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(data) == 0 {
		lines = nil
	}
	if after < 0 {
		after = 0
	}
	if after > len(lines) {
		after = len(lines)
	}
	chunk := lines[after:]
	if limit > 0 && len(chunk) > limit {
		chunk = chunk[:limit]
	}
	return chunk, after + len(chunk), nil
}

func (s *Store) exact(id string) (*Record, error) {
	rec := s.all[id]
	if rec == nil {
		return nil, ErrNotFound
	}
	return rec, nil
}

func (s *Store) resolve(selector string) (*Record, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return nil, ErrNotFound
	}
	if rec := s.all[selector]; rec != nil {
		return rec, nil
	}
	var match *Record
	for id, rec := range s.all {
		if strings.HasPrefix(id, selector) {
			if match != nil {
				return nil, ErrAmbiguous
			}
			match = rec
		}
	}
	if match == nil {
		return nil, ErrNotFound
	}
	return match, nil
}

func (s *Store) save(rec *Record) error {
	dir := filepath.Join(s.dir, "jobs", rec.ID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeAtomic(filepath.Join(dir, "job.json"), data, 0o600)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".job-*.tmp")
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

func newID() (string, error) {
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("job id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// Public returns a copy safe to print.
func Public(rec Record) Record {
	rec.Config = rec.Config.Redacted()
	return rec
}
