package transfer

import (
	"context"
	"errors"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/metrics"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
	"github.com/GerhardOfRivia/bezorg/internal/retry"
)

type protocolNamer interface {
	Protocol() string
}

func (e *engine) proto() string {
	e.mu.Lock()
	observed := e.observed
	e.mu.Unlock()
	if observed != "" {
		return observed
	}
	return metrics.Protocol(e.cfg.Protocol)
}

func (e *engine) rememberProtocol(sess remote.Session) {
	observed := protocolOf(sess, e.cfg.Protocol)
	if observed == "unknown" || observed == "auto" {
		return
	}
	e.mu.Lock()
	if e.observed == "" {
		e.observed = observed
	}
	e.mu.Unlock()
}

func protocolOf(sess remote.Session, fallback string) string {
	if sess != nil {
		if namer, ok := sess.(protocolNamer); ok {
			if observed := metrics.Protocol(namer.Protocol()); observed != "unknown" && observed != "auto" {
				return observed
			}
		}
	}
	return metrics.Protocol(fallback)
}

func (e *engine) canRetry(err error, attempt int) bool {
	if err == nil {
		return false
	}
	if retry.Classify(err) == retry.KindFatal || attempt >= e.cfg.Retries {
		return false
	}
	return true
}

func (e *engine) noteFailure(scope string, err error, attempt int) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	proto := e.proto()
	metrics.ObserveError(proto, scope, err)
	if e.canRetry(err, attempt) {
		metrics.ObserveRetry(proto, scope, err)
	}
}

func (e *engine) failTransfer(ctx context.Context, started time.Time, err error) error {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return err
	}
	metrics.ObserveTransfer(e.proto(), "failed", time.Since(started))
	return err
}
