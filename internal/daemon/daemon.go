// Package daemon runs download jobs in the background and resumes them after
// a restart. The CLI talks to it through the control socket.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/control"
	"github.com/GerhardOfRivia/bezorg/internal/dialer"
	"github.com/GerhardOfRivia/bezorg/internal/events"
	"github.com/GerhardOfRivia/bezorg/internal/job"
	"github.com/GerhardOfRivia/bezorg/internal/lock"
	"github.com/GerhardOfRivia/bezorg/internal/metrics"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
	"github.com/GerhardOfRivia/bezorg/internal/retry"
	"github.com/GerhardOfRivia/bezorg/internal/transfer"
	"github.com/GerhardOfRivia/bezorg/internal/webhook"
	"github.com/GerhardOfRivia/bezorg/internal/webui"
)

// Options configures a daemon process.
type Options struct {
	StateDir      string
	Socket        string
	Logger        *slog.Logger
	Version       string
	MaxJobs       int
	WebListen     string
	WebPublicRead bool
	MetricsListen string
}

// Run serves the control socket until ctx is canceled, then lets active
// transfers stop. Partial files stay on disk and interrupted jobs are queued
// again the next time the daemon starts.
func Run(ctx context.Context, opt Options) error {
	if opt.Logger == nil {
		opt.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	if opt.MaxJobs < 1 {
		opt.MaxJobs = 2
	}
	if opt.Version == "" {
		opt.Version = "dev"
	}
	metrics.SetVersion(opt.Version)
	if err := os.MkdirAll(opt.StateDir, 0o700); err != nil {
		return err
	}
	stateLock, err := lock.Exclusive(opt.StateDir + "/daemon.lock")
	if err != nil {
		return fmt.Errorf("state directory %s is in use: %w", opt.StateDir, err)
	}
	defer stateLock.Close()

	store, err := job.Open(opt.StateDir, opt.Logger)
	if err != nil {
		return err
	}
	eventLog, err := events.Open(filepath.Join(opt.StateDir, "events"), opt.Logger)
	if err != nil {
		return err
	}
	hookStore, err := webhook.Open(filepath.Join(opt.StateDir, "webhooks"))
	if err != nil {
		return err
	}
	if err := hookStore.Reconcile(eventLog.Snapshot()); err != nil {
		return err
	}
	manager := newManager(store, eventLog, hookStore, opt)
	server, err := control.Listen(opt.Socket, manager)
	if err != nil {
		return err
	}
	var webServer *webui.Server
	if address := strings.TrimSpace(opt.WebListen); address != "" {
		webServer, err = webui.NewServer(address, server.Path()+".web-token", manager, opt.Logger, opt.WebPublicRead)
		if err != nil {
			shutCtx, cancelListen := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelListen()
			_ = server.Shutdown(shutCtx)
			return err
		}
	}
	metricsServer, err := metrics.Listen(opt.MetricsListen, opt.Logger)
	if err != nil {
		if webServer != nil {
			_ = webServer.Close()
		}
		shutCtx, cancelListen := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelListen()
		_ = server.Shutdown(shutCtx)
		return err
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve()
	}()
	var webErr <-chan error
	if webServer != nil {
		ch := make(chan error, 1)
		go func() {
			ch <- webServer.Serve(ctx)
		}()
		webErr = ch
	}
	var metricsErr <-chan error
	if metricsServer != nil {
		ch := make(chan error, 1)
		go func() {
			ch <- metricsServer.Serve(ctx)
		}()
		metricsErr = ch
	}
	opt.Logger.Info("daemon listening", "socket", server.Path(), "state", opt.StateDir, "max_jobs", opt.MaxJobs)
	if webServer != nil {
		opt.Logger.Info("web dashboard listening", "address", webServer.Address(), "token_file", webServer.TokenPath(), "public_read", opt.WebPublicRead)
	}
	if metricsServer != nil {
		opt.Logger.Info("metrics listening", "address", metricsServer.Address())
	}
	manager.resume()
	go manager.dispatch(ctx)
	go webhook.Run(ctx, hookStore, opt.Logger)

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		runErr = err
	case err := <-webErr:
		runErr = err
	case err := <-metricsErr:
		runErr = err
	}
	if webServer != nil {
		if err := webServer.Close(); err != nil && runErr == nil {
			runErr = err
		}
	}
	if metricsServer != nil {
		if err := metricsServer.Close(); err != nil && runErr == nil {
			runErr = err
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	manager.stop()
	manager.wait(shutdownCtx)
	if err := server.Shutdown(shutdownCtx); err != nil && runErr == nil {
		runErr = err
	}
	if runErr != nil {
		return runErr
	}
	opt.Logger.Info("daemon stopped")
	return nil
}

type manager struct {
	store     *job.Store
	events    *events.Log
	hooks     *webhook.Store
	log       *slog.Logger
	version   string
	socket    string
	stateDir  string
	startedAt time.Time
	max       int

	mu       sync.Mutex
	cond     *sync.Cond
	queue    []string
	active   int
	running  map[string]context.CancelFunc
	canceled map[string]struct{}
	stopping bool
	wg       sync.WaitGroup
}

func newManager(store *job.Store, eventLog *events.Log, hookStore *webhook.Store, opt Options) *manager {
	m := &manager{
		store:     store,
		events:    eventLog,
		hooks:     hookStore,
		log:       opt.Logger,
		version:   opt.Version,
		socket:    opt.Socket,
		stateDir:  opt.StateDir,
		startedAt: time.Now().UTC(),
		max:       opt.MaxJobs,
		running:   map[string]context.CancelFunc{},
		canceled:  map[string]struct{}{},
	}
	m.cond = sync.NewCond(&m.mu)
	metrics.SetJobLimit(opt.MaxJobs)
	return m
}

func (m *manager) resume() {
	for _, rec := range m.store.List() {
		switch rec.State {
		case job.StateQueued, job.StateRunning, job.StateInterrupted:
			previous := rec.State
			if rec.State != job.StateQueued {
				if updated, err := m.store.Update(rec.ID, func(rec *job.Record) {
					rec.State = job.StateQueued
					rec.FinishedAt = nil
					rec.Error = ""
				}); err == nil {
					rec = updated
				}
			}
			m.enqueue(rec.ID)
			if previous != job.StateQueued {
				m.emit(rec, events.DownloadQueued, "")
			}
			m.log.Info("resumed job", "job", rec.ID, "remote", rec.Config.Remote)
		}
	}
}

func (m *manager) enqueue(id string) {
	m.mu.Lock()
	m.queue = append(m.queue, id)
	m.publishLocked()
	m.cond.Broadcast()
	m.mu.Unlock()
}

func (m *manager) publishLocked() {
	metrics.SetJobs(m.active, len(m.queue))
}

func (m *manager) dispatch(ctx context.Context) {
	go func() {
		<-ctx.Done()
		m.mu.Lock()
		m.cond.Broadcast()
		m.mu.Unlock()
	}()
	for {
		id, ok := m.next(ctx)
		if !ok {
			return
		}
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			defer func() {
				m.mu.Lock()
				m.active--
				m.publishLocked()
				m.cond.Broadcast()
				m.mu.Unlock()
			}()
			m.execute(ctx, id)
		}()
	}
}

func (m *manager) next(ctx context.Context) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if m.stopping || ctx.Err() != nil {
			return "", false
		}
		if m.active < m.max && len(m.queue) > 0 {
			id := m.queue[0]
			m.queue = m.queue[1:]
			m.active++
			m.publishLocked()
			return id, true
		}
		m.cond.Wait()
	}
}

func (m *manager) execute(parent context.Context, id string) {
	started := time.Now()
	if m.finishIfCanceled(id) {
		m.observeJob(id, "canceled", started)
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.mu.Lock()
	m.running[id] = cancel
	_, userCanceled := m.canceled[id]
	stopping := m.stopping
	m.mu.Unlock()
	defer func() {
		cancel()
		m.mu.Lock()
		delete(m.running, id)
		m.mu.Unlock()
	}()
	if userCanceled || stopping {
		m.finishIfCanceled(id)
		if stopping && !userCanceled {
			m.emit(m.mark(id, job.StateInterrupted, 4, "daemon stopped"), events.DownloadInterrupted, "")
			m.observeJob(id, "interrupted", started)
		} else {
			m.observeJob(id, "canceled", started)
		}
		return
	}

	rec, err := m.store.Get(id)
	if err != nil {
		m.log.Error("job disappeared", "job", id, "error", err)
		m.observeJob(id, "failed", started)
		return
	}
	now := time.Now().UTC()
	if updated, err := m.store.Update(id, func(rec *job.Record) {
		rec.State = job.StateRunning
		if rec.StartedAt == nil {
			rec.StartedAt = &now
		}
		rec.FinishedAt = nil
		rec.Error = ""
		rec.ExitCode = -1
	}); err == nil {
		rec = updated
		m.emit(updated, events.DownloadStarted, "")
	}

	logger, closeLog := m.jobLogger(rec)
	defer closeLog()
	var lastProgress time.Time
	var lastDone int
	result, runErr := transfer.Run(ctx, transfer.Request{
		Config:     rec.Config,
		JobID:      rec.ID,
		ExternalID: rec.ExternalID,
		Logger:     logger,
		Dial: func(ctx context.Context, cfg config.Config) (remote.Session, error) {
			return dialer.Dial(ctx, cfg, logger)
		},
		Sum: func(ctx context.Context, cfg config.Config) (remote.Checksummer, error) {
			if cfg.Checksum == config.ChecksumOff {
				return nil, nil
			}
			return dialer.Checksummer(ctx, cfg, logger)
		},
		OnProgress: func(snap transfer.Snapshot) {
			if snap.Phase == "downloading" && time.Since(lastProgress) < time.Second && snap.FilesDone == lastDone {
				return
			}
			lastProgress = time.Now()
			lastDone = snap.FilesDone
			_, _ = m.store.Update(id, func(rec *job.Record) {
				rec.Progress = snap
			})
		},
	})

	if m.wasCanceled(id) {
		m.mark(id, job.StateCanceled, 4, "canceled")
		m.observeJob(id, "canceled", started)
		return
	}
	if m.isStopping() || errors.Is(parent.Err(), context.Canceled) || errors.Is(runErr, context.Canceled) {
		m.emit(m.mark(id, job.StateInterrupted, 4, "interrupted"), events.DownloadInterrupted, "")
		m.observeJob(id, "interrupted", started)
		return
	}
	if runErr == nil && result.FilesFailed == 0 {
		updated, _ := m.store.Update(id, func(rec *job.Record) {
			rec.State = job.StateCompleted
			rec.Progress = result.Snapshot
			rec.Error = ""
			rec.ExitCode = 0
			finished := time.Now().UTC()
			rec.FinishedAt = &finished
		})
		m.emit(updated, events.DownloadSucceeded, result.Manifest)
		m.log.Info("job complete", "job", id, "files", result.FilesDone)
		m.observeJob(id, "completed", started)
		return
	}
	message := "download failed"
	if runErr != nil {
		message = runErr.Error()
	}
	permanent := result.Permanent || retry.Classify(runErr) == retry.KindFatal
	failure := jobFailure(permanent, runErr)
	metrics.ObserveError(rec.Config.Protocol, "job", failure)
	if !permanent && rec.Attempt < rec.Config.JobRetries {
		next := rec.Attempt + 1
		if updated, err := m.store.Update(id, func(rec *job.Record) {
			rec.State = job.StateQueued
			rec.Attempt = next
			rec.Progress = result.Snapshot
			rec.Error = message
		}); err == nil {
			m.emit(updated, events.DownloadRetried, "")
		}
		delay := retry.Backoff(next, rec.Config.JobRetryBase.Std(), rec.Config.JobRetryMax.Std())
		m.log.Warn("job will retry", "job", id, "attempt", next, "delay", delay, "error", message)
		if err := retry.Sleep(ctx, delay); err != nil {
			if m.wasCanceled(id) {
				m.mark(id, job.StateCanceled, 4, "canceled")
				m.observeJob(id, "canceled", started)
				return
			}
			m.emit(m.mark(id, job.StateInterrupted, 4, "interrupted"), events.DownloadInterrupted, "")
			m.observeJob(id, "interrupted", started)
			return
		}
		if m.wasCanceled(id) || m.isStopping() {
			if m.wasCanceled(id) {
				m.mark(id, job.StateCanceled, 4, "canceled")
				m.observeJob(id, "canceled", started)
			} else {
				m.emit(m.mark(id, job.StateInterrupted, 4, "interrupted"), events.DownloadInterrupted, "")
				m.observeJob(id, "interrupted", started)
			}
			return
		}
		metrics.ObserveRetry(rec.Config.Protocol, "job", failure)
		m.observeJob(id, "retried", started)
		m.enqueue(id)
		return
	}
	updated, _ := m.store.Update(id, func(rec *job.Record) {
		rec.State = job.StateFailed
		rec.Progress = result.Snapshot
		rec.Error = message
		rec.ExitCode = 1
		finished := time.Now().UTC()
		rec.FinishedAt = &finished
	})
	m.emit(updated, events.DownloadFailed, "")
	m.log.Error("job failed", "job", id, "error", message)
	m.observeJob(id, "failed", started)
}

func (m *manager) observeJob(id, outcome string, started time.Time) {
	protocol := "unknown"
	if rec, err := m.store.Get(id); err == nil {
		protocol = rec.Config.Protocol
	}
	metrics.ObserveJob(protocol, outcome, time.Since(started))
}

func jobFailure(permanent bool, runErr error) error {
	if permanent {
		return retry.ErrPermanent
	}
	if runErr != nil {
		return runErr
	}
	return errors.New("download failed")
}

func (m *manager) jobLogger(rec job.Record) (*slog.Logger, func()) {
	file, err := os.OpenFile(m.store.LogPath(rec.ID), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		m.log.Warn("job log unavailable", "job", rec.ID, "error", err)
		return m.log.With("job", rec.ID), func() {}
	}
	level := slog.LevelInfo
	switch rec.Config.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	var handler slog.Handler
	options := &slog.HandlerOptions{Level: level}
	if rec.Config.LogFormat == "json" {
		handler = slog.NewJSONHandler(file, options)
	} else {
		handler = slog.NewTextHandler(file, options)
	}
	logger := slog.New(&teeHandler{a: m.log.With("job", rec.ID).Handler(), b: handler}).With("job", rec.ID)
	return logger, func() { _ = file.Close() }
}

func (m *manager) finishIfCanceled(id string) bool {
	if !m.wasCanceled(id) {
		return false
	}
	m.mark(id, job.StateCanceled, 4, "canceled")
	return true
}

func (m *manager) wasCanceled(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.canceled[id]
	return ok
}

func (m *manager) isStopping() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopping
}

func (m *manager) mark(id, state string, code int, message string) job.Record {
	updated, err := m.store.Update(id, func(rec *job.Record) {
		rec.State = state
		rec.ExitCode = code
		rec.Error = message
		finished := time.Now().UTC()
		rec.FinishedAt = &finished
	})
	if err != nil {
		return job.Record{ID: id, State: state, Error: message, ExitCode: code}
	}
	return updated
}

func (m *manager) emit(rec job.Record, kind, manifest string) {
	if m.events == nil || rec.ID == "" {
		return
	}
	recorded, err := m.events.Append(events.Event{
		Type:         kind,
		JobID:        rec.ID,
		ExternalID:   rec.ExternalID,
		Remote:       rec.Config.Remote,
		Local:        rec.Config.Local,
		Status:       rec.State,
		Attempts:     rec.Attempt,
		Bytes:        rec.Progress.BytesDone,
		JobCreatedAt: rec.CreatedAt,
		UpdatedAt:    rec.UpdatedAt,
		StartedAt:    rec.StartedAt,
		FinishedAt:   rec.FinishedAt,
		Manifest:     manifest,
	})
	if err != nil {
		m.log.Warn("record download event", "job", rec.ID, "type", kind, "error", err)
		return
	}
	if m.hooks == nil {
		return
	}
	if err := m.hooks.Enqueue(recorded); err != nil {
		m.log.Warn("queue webhook delivery", "job", rec.ID, "type", kind, "error", err)
	}
}

func (m *manager) stop() {
	m.mu.Lock()
	m.stopping = true
	for _, cancel := range m.running {
		cancel()
	}
	m.cond.Broadcast()
	m.mu.Unlock()
}

func (m *manager) wait(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		m.log.Warn("daemon shutdown timed out with jobs still stopping")
	}
}

func (m *manager) Submit(_ context.Context, cfg config.Config) (job.Record, error) {
	cfg, err := config.Validate(cfg)
	if err != nil {
		return job.Record{}, err
	}
	rec, err := m.store.Create(cfg)
	if err != nil {
		return job.Record{}, err
	}
	m.enqueue(rec.ID)
	m.emit(rec, events.DownloadQueued, "")
	m.log.Info("job queued", "job", rec.ID, "host", cfg.Host, "remote", cfg.Remote)
	return job.Public(rec), nil
}

func (m *manager) List(_ context.Context, externalID string) ([]job.Record, error) {
	all, err := m.store.ListByExternalID(externalID)
	if err != nil {
		return nil, err
	}
	for i := range all {
		all[i] = job.Public(all[i])
	}
	return all, nil
}

func (m *manager) Get(_ context.Context, selector string) (job.Record, error) {
	rec, err := m.store.Get(selector)
	if err != nil {
		return job.Record{}, err
	}
	return job.Public(rec), nil
}

func (m *manager) Cancel(_ context.Context, selector string) (job.Record, error) {
	rec, err := m.store.Get(selector)
	if err != nil {
		return job.Record{}, err
	}
	if job.Terminal(rec.State) && rec.State != job.StateInterrupted {
		return job.Public(rec), nil
	}
	m.mu.Lock()
	m.canceled[rec.ID] = struct{}{}
	cancel := m.running[rec.ID]
	filtered := m.queue[:0]
	queued := false
	for _, id := range m.queue {
		if id == rec.ID {
			queued = true
			continue
		}
		filtered = append(filtered, id)
	}
	m.queue = filtered
	m.publishLocked()
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if queued && cancel == nil {
		m.mark(rec.ID, job.StateCanceled, 4, "canceled")
		started := rec.CreatedAt
		if started.IsZero() {
			started = time.Now()
		}
		m.observeJob(rec.ID, "canceled", started)
	}
	updated, err := m.store.Get(rec.ID)
	if err != nil {
		return job.Record{}, err
	}
	return job.Public(updated), nil
}

func (m *manager) Retry(_ context.Context, selector string) (job.Record, error) {
	rec, err := m.store.Get(selector)
	if err != nil {
		return job.Record{}, err
	}
	switch rec.State {
	case job.StateFailed, job.StateCanceled, job.StateInterrupted:
	default:
		return job.Record{}, fmt.Errorf("job %s is %s and cannot be retried", rec.ID, rec.State)
	}
	m.mu.Lock()
	delete(m.canceled, rec.ID)
	m.mu.Unlock()
	updated, err := m.store.Update(rec.ID, func(rec *job.Record) {
		rec.State = job.StateQueued
		rec.Error = ""
		rec.ExitCode = -1
		rec.FinishedAt = nil
		rec.Attempt = 0
	})
	if err != nil {
		return job.Record{}, err
	}
	m.emit(updated, events.DownloadRetried, "")
	m.enqueue(rec.ID)
	return job.Public(updated), nil
}

func (m *manager) Logs(_ context.Context, selector string, after int) ([]string, int, error) {
	rec, err := m.store.Get(selector)
	if err != nil {
		return nil, after, err
	}
	return m.store.Logs(rec.ID, after, 1000)
}

func (m *manager) CreateWebhook(ctx context.Context, params webhook.Params) (webhook.Hook, error) {
	if m.hooks == nil {
		return webhook.Hook{}, errors.New("webhooks are not configured")
	}
	if m.events != nil {
		params.CreatedSeq = m.events.HighWater()
	}
	return m.hooks.Create(ctx, params)
}

func (m *manager) ListWebhooks(ctx context.Context) ([]webhook.Hook, error) {
	if m.hooks == nil {
		return nil, errors.New("webhooks are not configured")
	}
	return m.hooks.List(ctx)
}

func (m *manager) DeleteWebhook(ctx context.Context, id int64) error {
	if m.hooks == nil {
		return errors.New("webhooks are not configured")
	}
	return m.hooks.Delete(ctx, id)
}

func (m *manager) ListWebhookDeliveries(ctx context.Context, id int64, status string, limit int) ([]webhook.Delivery, error) {
	if m.hooks == nil {
		return nil, errors.New("webhooks are not configured")
	}
	return m.hooks.Deliveries(ctx, id, status, limit)
}

func (m *manager) Events(_ context.Context, after int64, limit int) ([]events.Event, error) {
	if m.events == nil {
		return nil, errors.New("event log is not open")
	}
	return m.events.List(after, limit)
}

func (m *manager) System(context.Context) (control.System, error) {
	m.mu.Lock()
	active := m.active
	queued := len(m.queue)
	m.mu.Unlock()
	return control.System{
		Version:   m.version,
		Socket:    m.socket,
		StateDir:  m.stateDir,
		Active:    active,
		Queued:    queued,
		StartedAt: m.startedAt,
	}, nil
}

type teeHandler struct {
	a slog.Handler
	b slog.Handler
}

func (t *teeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return t.a.Enabled(ctx, level) || t.b.Enabled(ctx, level)
}

func (t *teeHandler) Handle(ctx context.Context, record slog.Record) error {
	errA := t.a.Handle(ctx, record.Clone())
	errB := t.b.Handle(ctx, record.Clone())
	return errors.Join(errA, errB)
}

func (t *teeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &teeHandler{a: t.a.WithAttrs(attrs), b: t.b.WithAttrs(attrs)}
}

func (t *teeHandler) WithGroup(name string) slog.Handler {
	return &teeHandler{a: t.a.WithGroup(name), b: t.b.WithGroup(name)}
}
