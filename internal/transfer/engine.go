package transfer

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
	"github.com/GerhardOfRivia/bezorg/internal/retry"
	"golang.org/x/time/rate"
)

// Snapshot is a point-in-time view of a running or finished job.
type Snapshot struct {
	Phase        string         `json:"phase"`
	FilesTotal   int            `json:"files_total"`
	FilesDone    int            `json:"files_done"`
	FilesSkipped int            `json:"files_skipped"`
	FilesFailed  int            `json:"files_failed"`
	BytesTotal   int64          `json:"bytes_total"`
	BytesDone    int64          `json:"bytes_done"`
	InFlight     []FileProgress `json:"in_flight,omitempty"`
	LastError    string         `json:"last_error,omitempty"`
	Permanent    bool           `json:"permanent"`
	Failures     []FileFailure  `json:"failures,omitempty"`
}

// FileProgress is one file currently being written.
type FileProgress struct {
	Path  string `json:"path"`
	Size  int64  `json:"size"`
	Have  int64  `json:"have"`
	Large bool   `json:"large"`
}

// FileFailure records one object that did not complete.
type FileFailure struct {
	Path  string `json:"path"`
	Error string `json:"error"`
}

// Result is the terminal snapshot of a run.
type Result struct {
	Snapshot
	DryRun bool `json:"dry_run"`
}

// DialFunc opens one protocol session.
type DialFunc func(ctx context.Context, cfg config.Config) (remote.Session, error)

// SumFunc opens a server-side checksum helper. It may be nil.
type SumFunc func(ctx context.Context, cfg config.Config) (remote.Checksummer, error)

// Request is one engine invocation.
type Request struct {
	Config     config.Config
	Dial       DialFunc
	Sum        SumFunc
	Logger     *slog.Logger
	OnProgress func(Snapshot)
}

type hints interface {
	SetTransferSize(int64)
	Unusable() bool
}

// Run lists the remote path and mirrors it into the local directory.
// Completed files are renamed into place only after the size (and checksum,
// when one is available) checks succeed. Partial files are left beside the
// destination so a later run can resume.
func Run(ctx context.Context, req Request) (Result, error) {
	cfg, err := config.Validate(req.Config)
	if err != nil {
		return Result{}, err
	}
	logger := req.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if req.Dial == nil {
		return Result{}, errors.New("transfer: dial function is required")
	}
	e := &engine{
		cfg:      cfg,
		dial:     req.Dial,
		sum:      req.Sum,
		log:      logger,
		progress: req.OnProgress,
		inflight: map[string]*FileProgress{},
	}
	res, err := e.run(ctx)
	return res, err
}

type engine struct {
	cfg      config.Config
	dial     DialFunc
	sum      SumFunc
	log      *slog.Logger
	progress func(Snapshot)

	mu             sync.Mutex
	inflight       map[string]*FileProgress
	completedBytes int64
	snap           Snapshot
	limiter        *rate.Limiter
	bufSize        int
	sums           remote.Checksummer
	gate           *Gate
}

func (e *engine) run(ctx context.Context) (Result, error) {
	e.bufSize = 256 * 1024
	if e.cfg.Bandwidth > 0 {
		burst := e.bufSize
		if e.cfg.Bandwidth < int64(burst) {
			burst = int(e.cfg.Bandwidth)
		}
		if burst < 1 {
			burst = 1
		}
		e.bufSize = burst
		e.limiter = rate.NewLimiter(rate.Limit(e.cfg.Bandwidth), burst)
	}
	e.setPhase("listing")
	e.log.Info("listing remote path", "host", e.cfg.Host, "remote", e.cfg.Remote, "protocol", e.cfg.Protocol)

	listing, err := e.list(ctx)
	if err != nil {
		e.noteError(err, retry.Classify(err) == retry.KindFatal)
		e.emit()
		return e.result(false), err
	}
	files, err := e.prepare(listing)
	if err != nil {
		e.noteError(err, true)
		e.emit()
		return e.result(false), err
	}
	var total int64
	for _, file := range files {
		if file.size > 0 {
			total += file.size
		}
	}
	e.mu.Lock()
	e.snap.FilesTotal = len(files)
	e.snap.BytesTotal = total
	e.mu.Unlock()
	e.log.Info("listing complete", "files", len(files), "bytes", total)
	e.emit()

	if e.cfg.DryRun {
		for _, file := range files {
			e.log.Info("dry-run", "file", file.rel, "bytes", file.size, "large", file.large)
		}
		e.setPhase("done")
		return e.result(true), nil
	}

	if e.cfg.Checksum != config.ChecksumOff && e.sum != nil && len(files) > 0 {
		sums, sumErr := e.sum(ctx, e.cfg)
		if sumErr != nil {
			if e.cfg.Checksum == config.ChecksumRequire && retry.Classify(sumErr) == retry.KindFatal {
				e.noteError(sumErr, true)
				e.emit()
				return e.result(false), sumErr
			}
			e.log.Warn("server checksum helper unavailable; size checks still apply", "error", sumErr)
		} else if sums != nil {
			e.sums = sums
			defer sums.Close()
		}
	}

	if len(files) == 0 {
		e.setPhase("done")
		e.log.Info("remote path is empty", "remote", e.cfg.Remote)
		return e.result(false), nil
	}

	workers := e.cfg.Concurrency
	if workers > len(files) {
		workers = len(files)
	}
	e.gate = newGate(e.cfg.Concurrency)
	queue := newWorkQueue(files, e.cfg.Concurrency, e.cfg.EffectiveLarge())
	go func() {
		<-ctx.Done()
		queue.wake()
	}()

	e.setPhase("downloading")
	stopProgress := make(chan struct{})
	go e.progressLoop(ctx, stopProgress)
	defer close(stopProgress)

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			if err := e.worker(ctx, index, queue); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	e.setPhase("done")

	var runErr error
	for err := range errCh {
		runErr = errors.Join(runErr, err)
	}
	if ctx.Err() != nil {
		return e.result(false), ctx.Err()
	}
	result := e.result(false)
	if result.FilesFailed > 0 {
		if runErr == nil {
			runErr = fmt.Errorf("transfer: %d file(s) failed", result.FilesFailed)
		}
		return result, runErr
	}
	e.log.Info("download complete", "files", result.FilesDone, "skipped", result.FilesSkipped, "bytes", result.BytesDone)
	return result, nil
}

func (e *engine) list(ctx context.Context) (remote.Listing, error) {
	var last error
	for attempt := 1; attempt <= e.cfg.Retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return remote.Listing{}, err
		}
		sess, err := e.dial(ctx, e.cfg)
		if err != nil {
			last = err
			e.log.Warn("list dial failed", "attempt", attempt, "error", err)
			if retry.Classify(err) == retry.KindFatal || attempt == e.cfg.Retries {
				return remote.Listing{}, err
			}
			if err := retry.Sleep(ctx, retry.Backoff(attempt, e.cfg.RetryBase.Std(), e.cfg.RetryMax.Std())); err != nil {
				return remote.Listing{}, err
			}
			continue
		}
		listing, err := sess.List(ctx, e.cfg.Remote)
		_ = sess.Close()
		if err == nil {
			return listing, nil
		}
		last = err
		e.log.Warn("list failed", "attempt", attempt, "error", err)
		if retry.Classify(err) == retry.KindFatal || attempt == e.cfg.Retries {
			return remote.Listing{}, err
		}
		if err := retry.Sleep(ctx, retry.Backoff(attempt, e.cfg.RetryBase.Std(), e.cfg.RetryMax.Std())); err != nil {
			return remote.Listing{}, err
		}
	}
	if last == nil {
		last = errors.New("transfer: list failed")
	}
	return remote.Listing{}, last
}

func (e *engine) prepare(listing remote.Listing) ([]remoteFile, error) {
	files := make([]remoteFile, 0, len(listing.Files))
	localIsDir := e.localIsDir()
	for _, file := range listing.Files {
		rel := file.Rel
		if file.Local == "" {
			cleaned, err := CleanRel(rel)
			if err != nil {
				return nil, fmt.Errorf("transfer: %s: %w", file.Remote, err)
			}
			rel = cleaned
		}
		item := remoteFile{
			rel:    rel,
			remote: file.Remote,
			local:  file.Local,
			size:   file.Size,
		}
		if listing.RootIsFile && len(listing.Files) == 1 && !localIsDir && e.cfg.Local != "" {
			item.local = e.cfg.Local
			item.rel = path.Base(file.Remote)
		}
		item.large = IsLarge(item.rel, item.size, e.cfg.LargeMinBytes)
		files = append(files, item)
	}
	return files, nil
}

func (e *engine) localIsDir() bool {
	if e.cfg.Local == "" {
		return true
	}
	if strings.HasSuffix(e.cfg.Local, "/") || strings.HasSuffix(e.cfg.Local, string(os.PathSeparator)) {
		return true
	}
	info, err := os.Stat(e.cfg.Local)
	return err == nil && info.IsDir()
}

func (e *engine) worker(ctx context.Context, index int, queue *workQueue) error {
	if index > 0 {
		stagger := time.Duration(index) * 100 * time.Millisecond
		if stagger > 2*time.Second {
			stagger = 2 * time.Second
		}
		if err := retry.Sleep(ctx, stagger); err != nil {
			return err
		}
	}
	var sess remote.Session
	held := false
	defer func() {
		if sess != nil {
			_ = sess.Close()
		}
		if held {
			e.gate.Release()
		}
	}()
	for {
		file, ok := queue.Pop(ctx)
		if !ok {
			return ctx.Err()
		}
		err := e.transfer(ctx, &sess, &held, file)
		queue.Done(file.large)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			e.failFile(file, err)
		}
	}
}

func (e *engine) transfer(ctx context.Context, sess *remote.Session, held *bool, file remoteFile) error {
	final, partial := e.paths(file)
	var last error
	restart := false
	for attempt := 1; attempt <= e.cfg.Retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.ensureSession(ctx, sess, held); err != nil {
			last = err
			e.log.Warn("dial failed", "file", file.rel, "attempt", attempt, "error", err)
			if retry.Classify(err) == retry.KindFatal || attempt == e.cfg.Retries {
				return err
			}
			if err := retry.Sleep(ctx, retry.Backoff(attempt, e.cfg.RetryBase.Std(), e.cfg.RetryMax.Std())); err != nil {
				return err
			}
			continue
		}
		err := e.once(ctx, *sess, file, final, partial, restart)
		if err == nil {
			if sessionUnusable(*sess) {
				e.dropSession(sess, held)
			}
			return nil
		}
		last = err
		kind := retry.Classify(err)
		if kind == retry.KindRestart {
			restart = true
		}
		e.log.Warn("file attempt failed", "file", file.rel, "attempt", attempt, "error", err)
		if kind == retry.KindFatal || attempt == e.cfg.Retries {
			return err
		}
		if kind != retry.KindRestart || sessionUnusable(*sess) {
			e.dropSession(sess, held)
		}
		if kind == retry.KindLimit {
			e.gate.Penalize(ctx, 30*time.Second)
		}
		if err := retry.Sleep(ctx, retry.Backoff(attempt, e.cfg.RetryBase.Std(), e.cfg.RetryMax.Std())); err != nil {
			return err
		}
	}
	if last == nil {
		last = errors.New("transfer failed")
	}
	return last
}

func (e *engine) ensureSession(ctx context.Context, sess *remote.Session, held *bool) error {
	if *sess != nil && !sessionUnusable(*sess) {
		return nil
	}
	e.dropSession(sess, held)
	if err := e.gate.Acquire(ctx); err != nil {
		return err
	}
	*held = true
	next, err := e.dial(ctx, e.cfg)
	if err != nil {
		e.gate.Release()
		*held = false
		if retry.Classify(err) == retry.KindLimit {
			e.gate.Penalize(ctx, 30*time.Second)
		}
		return err
	}
	*sess = next
	return nil
}

func (e *engine) dropSession(sess *remote.Session, held *bool) {
	if *sess != nil {
		_ = (*sess).Close()
		*sess = nil
	}
	if held != nil && *held {
		e.gate.Release()
		*held = false
	}
}

func (e *engine) once(ctx context.Context, sess remote.Session, file remoteFile, final, partial string, restart bool) error {
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return err
	}
	finalInfo, finalErr := os.Stat(final)
	partialInfo, partialErr := os.Stat(partial)
	plan := PlanLocal(finalErr == nil, sizeOf(finalInfo, finalErr), partialErr == nil, sizeOf(partialInfo, partialErr), file.size)
	if restart {
		plan = LocalPlan{Action: ActionFetch, Offset: 0}
	}
	switch plan.Action {
	case ActionSkip:
		if err := e.maybeChecksum(ctx, sess, file, final); err != nil {
			return err
		}
		e.complete(file, file.size, true)
		e.log.Info("skip existing file", "file", file.rel, "bytes", file.size)
		return nil
	case ActionFinish:
		if err := e.finish(ctx, sess, file, final, partial); err != nil {
			return err
		}
		e.complete(file, countedSize(file, partial), false)
		return nil
	}

	if err := e.begin(file, plan.Offset); err != nil {
		return err
	}
	flags := os.O_CREATE | os.O_RDWR
	fh, err := os.OpenFile(partial, flags, 0o644)
	if err != nil {
		e.endInFlight(file.rel)
		return err
	}
	defer fh.Close()
	if plan.Offset == 0 {
		if err := fh.Truncate(0); err != nil {
			e.endInFlight(file.rel)
			return err
		}
	}
	if _, err := fh.Seek(plan.Offset, io.SeekStart); err != nil {
		e.endInFlight(file.rel)
		return err
	}
	if file.size >= 0 && plan.Offset == file.size {
		if err := fh.Sync(); err != nil {
			e.endInFlight(file.rel)
			return err
		}
		if err := e.finish(ctx, sess, file, final, partial); err != nil {
			e.endInFlight(file.rel)
			return err
		}
		e.complete(file, countedSize(file, partial), false)
		return nil
	}

	if sized, ok := sess.(interface{ SetTransferSize(int64) }); ok {
		sized.SetTransferSize(file.size)
	}
	body, err := sess.OpenAt(ctx, file.remote, plan.Offset)
	if err != nil {
		e.endInFlight(file.rel)
		if plan.Offset > 0 && restRejected(err) {
			return fmt.Errorf("server rejected resume: %w", retry.ErrRestart)
		}
		return err
	}
	written, copyErr := copyStream(ctx, fh, body, e.cfg.StallTimeout.Std(), e.limiter, e.bufSize, func(n int64) {
		e.addHave(file.rel, n)
	})
	closeErr := body.Close()
	if copyErr != nil {
		e.endInFlight(file.rel)
		return copyErr
	}
	if closeErr != nil && !errors.Is(closeErr, io.EOF) {
		e.log.Debug("data close", "file", file.rel, "error", closeErr)
	}
	if err := fh.Sync(); err != nil {
		e.endInFlight(file.rel)
		return err
	}
	info, err := fh.Stat()
	if err != nil {
		e.endInFlight(file.rel)
		return err
	}
	if file.size >= 0 && info.Size() != file.size {
		e.endInFlight(file.rel)
		return fmt.Errorf("short read for %s: got %d bytes, want %d (wrote %d this attempt)", file.rel, info.Size(), file.size, written)
	}
	if err := e.finish(ctx, sess, file, final, partial); err != nil {
		e.endInFlight(file.rel)
		return err
	}
	e.complete(file, countedSize(file, final), false)
	e.log.Info("file complete", "file", file.rel, "bytes", info.Size(), "resumed", plan.Offset > 0)
	return nil
}

func (e *engine) finish(ctx context.Context, sess remote.Session, file remoteFile, final, partial string) error {
	if err := e.maybeChecksum(ctx, sess, file, partial); err != nil {
		return err
	}
	if err := os.Rename(partial, final); err != nil {
		return err
	}
	syncDir(filepath.Dir(final))
	return nil
}

func (e *engine) maybeChecksum(ctx context.Context, sess remote.Session, file remoteFile, localPath string) error {
	if e.cfg.Checksum == config.ChecksumOff || isChecksumSidecar(file.rel) {
		return nil
	}
	sum, algo, ok, err := e.expectedSum(ctx, sess, file)
	if err != nil {
		return err
	}
	if !ok {
		if e.cfg.Checksum == config.ChecksumRequire {
			return fmt.Errorf("checksum required but unavailable for %s: %w", file.rel, retry.ErrPermanent)
		}
		return nil
	}
	got, err := hashFile(localPath, algo)
	if err != nil {
		return err
	}
	if !strings.EqualFold(got, sum) {
		_ = os.Remove(localPath)
		return fmt.Errorf("checksum mismatch for %s: %w", file.rel, retry.ErrRestart)
	}
	e.log.Debug("checksum ok", "file", file.rel, "algo", algo)
	return nil
}

func (e *engine) expectedSum(ctx context.Context, sess remote.Session, file remoteFile) (sum, algo string, ok bool, err error) {
	algo = e.cfg.ChecksumAlgo
	if algo == "" {
		algo = config.AlgoAuto
	}
	if e.sums != nil {
		sum, ok, err = e.sums.Sum(ctx, file.remote, algo)
		if err != nil {
			if retry.Classify(err) == retry.KindFatal && e.cfg.Checksum == config.ChecksumRequire {
				return "", algo, false, err
			}
			if retry.Transient(err) {
				return "", algo, false, err
			}
			ok = false
		}
		if ok {
			if algo == config.AlgoAuto {
				algo = algoFromSum(sum)
			}
			return strings.ToLower(sum), algo, true, nil
		}
	}
	return e.sidecar(ctx, sess, file, algo)
}

func (e *engine) sidecar(ctx context.Context, sess remote.Session, file remoteFile, algo string) (string, string, bool, error) {
	candidates := []struct {
		ext  string
		algo string
	}{
		{".sha256", config.AlgoSHA256},
		{".sha1", config.AlgoSHA1},
		{".md5", config.AlgoMD5},
	}
	if algo != config.AlgoAuto {
		filtered := candidates[:0]
		for _, candidate := range candidates {
			if candidate.algo == algo {
				filtered = append(filtered, candidate)
			}
		}
		candidates = filtered
	}
	for _, candidate := range candidates {
		body, err := sess.ReadSmall(ctx, file.remote+candidate.ext, 4096)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || isMissing(err) {
				continue
			}
			if retry.Transient(err) {
				return "", candidate.algo, false, err
			}
			continue
		}
		sum := parseSum(string(body))
		if sum == "" {
			continue
		}
		return sum, candidate.algo, true, nil
	}
	return "", algo, false, nil
}

func (e *engine) paths(file remoteFile) (final, partial string) {
	if file.local != "" {
		final = file.local
	} else {
		final = filepath.Join(e.cfg.Local, filepath.FromSlash(file.rel))
	}
	partial = final + e.cfg.TempSuffix
	return final, partial
}

func (e *engine) begin(file remoteFile, have int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.inflight[file.rel] = &FileProgress{Path: file.rel, Size: file.size, Have: have, Large: file.large}
	return nil
}

func (e *engine) addHave(rel string, n int64) {
	e.mu.Lock()
	if fp := e.inflight[rel]; fp != nil {
		fp.Have += n
	}
	e.mu.Unlock()
}

func (e *engine) endInFlight(rel string) {
	e.mu.Lock()
	delete(e.inflight, rel)
	e.mu.Unlock()
}

func (e *engine) complete(file remoteFile, size int64, skipped bool) {
	e.mu.Lock()
	delete(e.inflight, file.rel)
	if size > 0 {
		e.completedBytes += size
	}
	e.snap.FilesDone++
	if skipped {
		e.snap.FilesSkipped++
	}
	e.mu.Unlock()
	e.emit()
}

func (e *engine) failFile(file remoteFile, err error) {
	e.mu.Lock()
	delete(e.inflight, file.rel)
	e.snap.FilesFailed++
	e.snap.LastError = err.Error()
	if retry.Classify(err) == retry.KindFatal {
		e.snap.Permanent = true
	}
	if len(e.snap.Failures) < 32 {
		e.snap.Failures = append(e.snap.Failures, FileFailure{Path: file.rel, Error: err.Error()})
	}
	e.mu.Unlock()
	e.log.Error("file failed", "file", file.rel, "error", err)
	e.emit()
}

func (e *engine) noteError(err error, permanent bool) {
	e.mu.Lock()
	e.snap.LastError = err.Error()
	if permanent {
		e.snap.Permanent = true
	}
	e.mu.Unlock()
}

func (e *engine) setPhase(phase string) {
	e.mu.Lock()
	e.snap.Phase = phase
	e.mu.Unlock()
	e.emit()
}

func (e *engine) emit() {
	if e.progress == nil {
		return
	}
	e.progress(e.snapshot())
}

func (e *engine) snapshot() Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := e.snap
	out.BytesDone = e.completedBytes
	if len(e.inflight) > 0 {
		out.InFlight = make([]FileProgress, 0, len(e.inflight))
		for _, fp := range e.inflight {
			out.InFlight = append(out.InFlight, *fp)
			out.BytesDone += fp.Have
		}
	}
	out.Failures = append([]FileFailure(nil), e.snap.Failures...)
	return out
}

func (e *engine) result(dry bool) Result {
	snap := e.snapshot()
	snap.Phase = "done"
	return Result{Snapshot: snap, DryRun: dry}
}

func (e *engine) progressLoop(ctx context.Context, stop <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var lastBytes int64
	lastAt := time.Now()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			snap := e.snapshot()
			elapsed := now.Sub(lastAt).Seconds()
			rateBytes := float64(0)
			if elapsed > 0 {
				rateBytes = float64(snap.BytesDone-lastBytes) / elapsed
			}
			lastBytes = snap.BytesDone
			lastAt = now
			e.log.Info("progress",
				"files", fmt.Sprintf("%d/%d", snap.FilesDone, snap.FilesTotal),
				"failed", snap.FilesFailed,
				"bytes", snap.BytesDone,
				"bytes_total", snap.BytesTotal,
				"rate_bps", int64(rateBytes),
				"inflight", len(snap.InFlight),
			)
			e.emit()
		}
	}
}

func sessionUnusable(sess remote.Session) bool {
	probe, ok := sess.(interface{ Unusable() bool })
	return ok && probe.Unusable()
}

func sizeOf(info os.FileInfo, err error) int64 {
	if err != nil || info == nil {
		return 0
	}
	return info.Size()
}

func countedSize(file remoteFile, path string) int64 {
	if file.size > 0 {
		return file.size
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func restRejected(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "rest") && (strings.Contains(msg, "not implemented") || strings.Contains(msg, "502") || strings.Contains(msg, "504"))
}

func isChecksumSidecar(rel string) bool {
	switch strings.ToLower(path.Ext(rel)) {
	case ".md5", ".sha1", ".sha256", ".sha512":
		return true
	default:
		return false
	}
}

func isMissing(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such file") || strings.Contains(msg, "not found") || strings.Contains(msg, "550")
}

func parseSum(text string) string {
	for _, field := range strings.Fields(text) {
		field = strings.Trim(field, "*")
		if isHex(field) && (len(field) == 32 || len(field) == 40 || len(field) == 64) {
			return strings.ToLower(field)
		}
	}
	return ""
}

func isHex(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f', r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	return true
}

func algoFromSum(sum string) string {
	switch len(strings.TrimSpace(sum)) {
	case 32:
		return config.AlgoMD5
	case 40:
		return config.AlgoSHA1
	case 64:
		return config.AlgoSHA256
	default:
		return config.AlgoSHA256
	}
}

func hashFile(path, algo string) (string, error) {
	fh, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fh.Close()
	var dig hash.Hash
	switch algo {
	case config.AlgoMD5:
		dig = md5.New()
	case config.AlgoSHA1:
		dig = sha1.New()
	default:
		dig = sha256.New()
	}
	if _, err := io.Copy(dig, fh); err != nil {
		return "", err
	}
	return hex.EncodeToString(dig.Sum(nil)), nil
}

func syncDir(dir string) {
	fh, err := os.Open(dir)
	if err != nil {
		return
	}
	defer fh.Close()
	_ = fh.Sync()
}
