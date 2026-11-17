// Package daemon runs download jobs in the background and resumes them after
// a restart. The CLI talks to it through the control socket.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/control"
	"github.com/GerhardOfRivia/bezorg/internal/dialer"
	"github.com/GerhardOfRivia/bezorg/internal/job"
	"github.com/GerhardOfRivia/bezorg/internal/lock"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
	"github.com/GerhardOfRivia/bezorg/internal/retry"
	"github.com/GerhardOfRivia/bezorg/internal/transfer"
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
	manager := newManager(store, opt)
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
	opt.Logger.Info("daemon listening", "socket", server.Path(), "state", opt.StateDir, "max_jobs", opt.MaxJobs)
	if webServer != nil {
		opt.Logger.Info("web dashboard listening", "address", webServer.Address(), "token_file", webServer.TokenPath(), "public_read", opt.WebPublicRead)
	}
	manager.resume()
	go manager.dispatch(ctx)

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		runErr = err
	case err := <-webErr:
		runErr = err
	}
	if webServer != nil {
		if err := webServer.Close(); err != nil && runErr == nil {
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

func newManager(store *job.Store, opt Options) *manager {
	m := &manager{
		store:     store,
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
	return m
}

func (m *manager) resume() {
	for _, rec := range m.store.List() {
		switch rec.State {
		case job.StateQueued, job.StateRunning, job.StateInterrupted:
			if rec.State != job.StateQueued {
				_, _ = m.store.Update(rec.ID, func(rec *job.Record) {
					rec.State = job.StateQueued
					rec.FinishedAt = nil
					rec.Error = ""
				})
			}
			m.enqueue(rec.ID)
			m.log.Info("resumed job", "job", rec.ID, "remote", rec.Config.Remote)
		}
	}
}

func (m *manager) enqueue(id string) {
	m.mu.Lock()
	m.queue = append(m.queue, id)
	m.cond.Broadcast()
	m.mu.Unlock()
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
			return id, true
		}
		m.cond.Wait()
	}
}

func (m *manager) execute(parent context.Context, id string) {
	if m.finishIfCanceled(id) {
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
			m.mark(id, job.StateInterrupted, 4, "daemon stopped")
		}
		return
	}

	rec, err := m.store.Get(id)
	if err != nil {
		m.log.Error("job disappeared", "job", id, "error", err)
		return
	}
	now := time.Now().UTC()
	_, _ = m.store.Update(id, func(rec *job.Record) {
		rec.State = job.StateRunning
		if rec.StartedAt == nil {
			rec.StartedAt = &now
		}
		rec.FinishedAt = nil
		rec.Error = ""
		rec.ExitCode = -1
	})

	logger, closeLog := m.jobLogger(rec)
	defer closeLog()
	var lastProgress time.Time
	var lastDone int
	result, runErr := transfer.Run(ctx, transfer.Request{
		Config: rec.Config,
		Logger: logger,
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
		return
	}
	if m.isStopping() || errors.Is(parent.Err(), context.Canceled) || errors.Is(runErr, context.Canceled) {
		m.mark(id, job.StateInterrupted, 4, "interrupted")
		return
	}
	if runErr == nil && result.FilesFailed == 0 {
		_, _ = m.store.Update(id, func(rec *job.Record) {
			rec.State = job.StateCompleted
			rec.Progress = result.Snapshot
			rec.Error = ""
			rec.ExitCode = 0
			finished := time.Now().UTC()
			rec.FinishedAt = &finished
		})
		m.log.Info("job complete", "job", id, "files", result.FilesDone)
		return
	}
	message := "download failed"
	if runErr != nil {
		message = runErr.Error()
	}
	permanent := result.Permanent || retry.Classify(runErr) == retry.KindFatal
	if !permanent && rec.Attempt < rec.Config.JobRetries {
		next := rec.Attempt + 1
		_, _ = m.store.Update(id, func(rec *job.Record) {
			rec.State = job.StateQueued
			rec.Attempt = next
			rec.Progress = result.Snapshot
			rec.Error = message
		})
		delay := retry.Backoff(next, rec.Config.JobRetryBase.Std(), rec.Config.JobRetryMax.Std())
		m.log.Warn("job will retry", "job", id, "attempt", next, "delay", delay, "error", message)
		if err := retry.Sleep(ctx, delay); err != nil {
			if m.wasCanceled(id) {
				m.mark(id, job.StateCanceled, 4, "canceled")
				return
			}
			m.mark(id, job.StateInterrupted, 4, "interrupted")
			return
		}
		if m.wasCanceled(id) || m.isStopping() {
			if m.wasCanceled(id) {
				m.mark(id, job.StateCanceled, 4, "canceled")
			} else {
				m.mark(id, job.StateInterrupted, 4, "interrupted")
			}
			return
		}
		m.enqueue(id)
		return
	}
	_, _ = m.store.Update(id, func(rec *job.Record) {
		rec.State = job.StateFailed
		rec.Progress = result.Snapshot
		rec.Error = message
		rec.ExitCode = 1
		finished := time.Now().UTC()
		rec.FinishedAt = &finished
	})
	m.log.Error("job failed", "job", id, "error", message)
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

func (m *manager) mark(id, state string, code int, message string) {
	_, _ = m.store.Update(id, func(rec *job.Record) {
		rec.State = state
		rec.ExitCode = code
		rec.Error = message
		finished := time.Now().UTC()
		rec.FinishedAt = &finished
	})
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
	m.log.Info("job queued", "job", rec.ID, "host", cfg.Host, "remote", cfg.Remote)
	return job.Public(rec), nil
}

func (m *manager) List(context.Context) ([]job.Record, error) {
	all := m.store.List()
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
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if queued && cancel == nil {
		m.mark(rec.ID, job.StateCanceled, 4, "canceled")
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
