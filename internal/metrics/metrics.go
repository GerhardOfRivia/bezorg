// Package metrics records Prometheus series for bezorg transfers and jobs.
//
// Label sets are fixed. Paths, hosts, and job ids are not labels. An
// unexpected label value is collapsed to "other", or to "unknown" for the
// protocol, so a caller cannot create a series per file.
package metrics

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/retry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	dto "github.com/prometheus/client_model/go"
)

// latencyBuckets spans a small-file listing through a multi-gigabyte raster
// on a slow link: 50ms, then up to two hours.
var latencyBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 7200}

var (
	registry = prometheus.NewRegistry()

	transfers = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bezorg_transfers_total",
		Help: "Files finished by a worker, by protocol and outcome (downloaded, skipped, or failed).",
	}, []string{"protocol", "outcome"})

	transferResumes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bezorg_transfer_resumes_total",
		Help: "Downloaded files that continued from an existing partial file.",
	}, []string{"protocol"})

	transferBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bezorg_transfer_bytes_total",
		Help: "Bytes read from remote servers. Skipped files and bytes already on disk are not included.",
	}, []string{"protocol"})

	transferDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bezorg_transfer_duration_seconds",
		Help:    "Seconds to finish one file, including retries of that file.",
		Buckets: latencyBuckets,
	}, []string{"protocol", "outcome"})

	transfersActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bezorg_transfers_active",
		Help: "Files currently in progress, including retry backoff, by protocol and lane (small or large).",
	}, []string{"protocol", "lane"})

	connections = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bezorg_connections_total",
		Help: "Dial attempts for listings and worker sessions, by protocol and result (success or error).",
	}, []string{"protocol", "result"})

	connectionsActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bezorg_connections_active",
		Help: "Worker sessions currently open. Listing connections are closed before this gauge changes.",
	}, []string{"protocol"})

	penalties = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bezorg_connection_penalties_total",
		Help: "Times a connection slot was held back after the server reported a session limit.",
	}, []string{"protocol"})

	errorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bezorg_errors_total",
		Help: "Classified failures by protocol, scope (dial, list, file, job), and kind (fatal, transient, limit, restart). Retried failures are included.",
	}, []string{"protocol", "scope", "kind"})

	retriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bezorg_retries_total",
		Help: "Retries scheduled after a classified failure, by protocol, scope, and kind.",
	}, []string{"protocol", "scope", "kind"})

	listingDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bezorg_listing_duration_seconds",
		Help:    "Seconds to list a remote tree, by protocol and result (success or error).",
		Buckets: latencyBuckets,
	}, []string{"protocol", "result"})

	queueFiles = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bezorg_queue_files",
		Help: "Remote files waiting for a worker, by lane (small or large).",
	}, []string{"lane"})

	jobsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "bezorg_jobs_active",
		Help: "Daemon jobs currently running.",
	})

	jobsQueued = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "bezorg_jobs_queued",
		Help: "Daemon jobs waiting for a free run slot.",
	})

	jobsLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "bezorg_jobs_limit",
		Help: "Maximum daemon jobs running at once. Zero when this process is not the daemon.",
	})

	jobsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bezorg_jobs_total",
		Help: "Daemon job attempts by protocol and outcome (completed, failed, canceled, interrupted, or retried).",
	}, []string{"protocol", "outcome"})

	jobDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bezorg_job_duration_seconds",
		Help:    "Seconds spent on one daemon job attempt, including retry backoff before the job is queued again.",
		Buckets: latencyBuckets,
	}, []string{"protocol", "outcome"})

	buildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bezorg_build_info",
		Help: "Build information for this process. The value is always 1.",
	}, []string{"version"})

	webhookDeliveries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bezorg_webhook_deliveries_total",
		Help: "Webhook HTTP tries by result (succeeded, retry, or dead). URLs are not labels.",
	}, []string{"result"})
)

func init() {
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		transfers,
		transferResumes,
		transferBytes,
		transferDuration,
		transfersActive,
		connections,
		connectionsActive,
		penalties,
		errorsTotal,
		retriesTotal,
		listingDuration,
		queueFiles,
		jobsActive,
		jobsQueued,
		jobsLimit,
		jobsTotal,
		jobDuration,
		buildInfo,
		webhookDeliveries,
	)
	SetVersion("dev")
}

// SetVersion records the running binary version on bezorg_build_info.
func SetVersion(version string) {
	version = strings.TrimSpace(version)
	if version == "" {
		version = "dev"
	}
	buildInfo.Reset()
	buildInfo.WithLabelValues(version).Set(1)
}

// Protocol maps a configured or detected protocol onto the fixed label set.
func Protocol(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case config.ProtocolFTP, config.ProtocolFTPS, config.ProtocolFTPSImplicit, config.ProtocolSFTP, config.ProtocolAuto:
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "unknown"
	}
}

// ObserveTransfer counts one finished file and records how long it took.
func ObserveTransfer(protocol, outcome string, elapsed time.Duration) {
	protocol = Protocol(protocol)
	outcome = fileOutcome(outcome)
	transfers.WithLabelValues(protocol, outcome).Inc()
	transferDuration.WithLabelValues(protocol, outcome).Observe(seconds(elapsed))
}

// Resume counts a download that continued from a partial file.
func Resume(protocol string) {
	transferResumes.WithLabelValues(Protocol(protocol)).Inc()
}

// AddBytes adds bytes actually read from a remote server.
func AddBytes(protocol string, n int64) {
	if n <= 0 {
		return
	}
	transferBytes.WithLabelValues(Protocol(protocol)).Add(float64(n))
}

// TransfersActive adjusts the number of files in progress.
func TransfersActive(protocol, lane string, delta int) {
	if delta == 0 {
		return
	}
	transfersActive.WithLabelValues(Protocol(protocol), laneOf(lane)).Add(float64(delta))
}

// Connection counts one dial attempt. result is success or error.
func Connection(protocol, result string) {
	connections.WithLabelValues(Protocol(protocol), resultOf(result)).Inc()
}

// ConnectionsActive adjusts the number of open worker sessions.
func ConnectionsActive(protocol string, delta int) {
	if delta == 0 {
		return
	}
	connectionsActive.WithLabelValues(Protocol(protocol)).Add(float64(delta))
}

// Penalty counts one connection slot pulled out of circulation.
func Penalty(protocol string) {
	penalties.WithLabelValues(Protocol(protocol)).Inc()
}

// ObserveError counts a classified failure. A nil or canceled error is ignored.
func ObserveError(protocol, scope string, err error) {
	if err == nil || isCanceled(err) {
		return
	}
	errorsTotal.WithLabelValues(Protocol(protocol), scopeOf(scope), kindOf(retry.Classify(err))).Inc()
}

// ObserveRetry counts a retry scheduled after err. A nil or canceled error is ignored.
func ObserveRetry(protocol, scope string, err error) {
	if err == nil || isCanceled(err) {
		return
	}
	retriesTotal.WithLabelValues(Protocol(protocol), scopeOf(scope), kindOf(retry.Classify(err))).Inc()
}

// ObserveListing records how long a remote listing took. result is success or error.
func ObserveListing(protocol, result string, elapsed time.Duration) {
	listingDuration.WithLabelValues(Protocol(protocol), resultOf(result)).Observe(seconds(elapsed))
}

// QueueAdd adjusts the number of files waiting for a worker.
func QueueAdd(lane string, delta int) {
	if delta == 0 {
		return
	}
	queueFiles.WithLabelValues(laneOf(lane)).Add(float64(delta))
}

// SetJobs publishes the daemon queue gauges.
func SetJobs(active, queued int) {
	jobsActive.Set(float64(active))
	jobsQueued.Set(float64(queued))
}

// SetJobLimit publishes the daemon's maximum number of concurrent jobs.
func SetJobLimit(limit int) {
	if limit < 0 {
		limit = 0
	}
	jobsLimit.Set(float64(limit))
}

// ObserveJob counts one daemon job attempt and records its duration.
func ObserveJob(protocol, outcome string, elapsed time.Duration) {
	protocol = Protocol(protocol)
	outcome = jobOutcome(outcome)
	jobsTotal.WithLabelValues(protocol, outcome).Inc()
	jobDuration.WithLabelValues(protocol, outcome).Observe(seconds(elapsed))
}

// WebhookDelivery counts one webhook HTTP try. result is succeeded, retry, or dead.
func WebhookDelivery(result string) {
	switch result {
	case "succeeded", "retry", "dead":
	default:
		result = "other"
	}
	webhookDeliveries.WithLabelValues(result).Inc()
}

// Sample returns the current value of a counter or gauge, or the number of
// observations of a histogram. It returns 0 when the series is absent.
func Sample(name string, labels map[string]string) float64 {
	families, err := registry.Gather()
	if err != nil {
		return 0
	}
	if labels == nil {
		labels = map[string]string{}
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if !labelsEqual(metric, labels) {
				continue
			}
			switch {
			case metric.GetCounter() != nil:
				return metric.GetCounter().GetValue()
			case metric.GetGauge() != nil:
				return metric.GetGauge().GetValue()
			case metric.GetHistogram() != nil:
				return float64(metric.GetHistogram().GetSampleCount())
			}
		}
	}
	return 0
}

func labelsEqual(metric *dto.Metric, labels map[string]string) bool {
	got := metric.GetLabel()
	if len(got) != len(labels) {
		return false
	}
	for _, label := range got {
		value, ok := labels[label.GetName()]
		if !ok || value != label.GetValue() {
			return false
		}
	}
	return true
}

func seconds(elapsed time.Duration) float64 {
	if elapsed < 0 {
		return 0
	}
	return elapsed.Seconds()
}

func isCanceled(err error) bool {
	return errors.Is(err, context.Canceled)
}

func fileOutcome(outcome string) string {
	switch outcome {
	case "downloaded", "skipped", "failed":
		return outcome
	default:
		return "other"
	}
}

func jobOutcome(outcome string) string {
	switch outcome {
	case "completed", "failed", "canceled", "interrupted", "retried":
		return outcome
	default:
		return "other"
	}
}

func scopeOf(scope string) string {
	switch scope {
	case "dial", "list", "file", "job":
		return scope
	default:
		return "other"
	}
}

func laneOf(lane string) string {
	switch lane {
	case "small", "large":
		return lane
	default:
		return "other"
	}
}

func resultOf(result string) string {
	switch result {
	case "success", "error":
		return result
	default:
		return "other"
	}
}

func kindOf(kind retry.Kind) string {
	switch kind {
	case retry.KindFatal:
		return "fatal"
	case retry.KindTransient:
		return "transient"
	case retry.KindLimit:
		return "limit"
	case retry.KindRestart:
		return "restart"
	default:
		return "other"
	}
}
