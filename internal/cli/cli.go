// Package cli implements the bezorg and bezorgd command lines.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/control"
	"github.com/GerhardOfRivia/bezorg/internal/daemon"
	"github.com/GerhardOfRivia/bezorg/internal/dialer"
	"github.com/GerhardOfRivia/bezorg/internal/format"
	"github.com/GerhardOfRivia/bezorg/internal/job"
	"github.com/GerhardOfRivia/bezorg/internal/metrics"
	"github.com/GerhardOfRivia/bezorg/internal/paths"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
	"github.com/GerhardOfRivia/bezorg/internal/transfer"
)

const (
	exitOK       = 0
	exitFailed   = 1
	exitUsage    = 2
	exitDaemon   = 3
	exitCanceled = 4
)

// Run executes a bezorg client command.
func Run(args []string, stdout, stderr io.Writer, version string) int {
	metrics.SetVersion(version)
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if len(args) == 0 {
		fmt.Fprint(stderr, clientUsage)
		return exitUsage
	}
	switch args[0] {
	case "help", "-h", "--help":
		fmt.Fprint(stdout, clientUsage)
		return exitOK
	case "version", "-version", "--version":
		fmt.Fprintf(stdout, "bezorg %s\n", version)
		return exitOK
	case "run":
		return runForeground(args[1:], stdout, stderr)
	case "fetch":
		return runFetch(args[1:], stdout, stderr, false)
	case "jobs":
		return runJobs(args[1:], stdout, stderr)
	case "status":
		return runStatus(args[1:], stdout, stderr)
	case "logs":
		return runLogs(args[1:], stdout, stderr)
	case "cancel":
		return runCancel(args[1:], stdout, stderr)
	case "retry":
		return runRetry(args[1:], stdout, stderr)
	case "wait":
		return runWait(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "bezorg: unknown command %q\n\n%s", args[0], clientUsage)
		return exitUsage
	}
}

// RunDaemon executes bezorgd.
func RunDaemon(args []string, stdout, stderr io.Writer, version string) int {
	metrics.SetVersion(version)
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if len(args) > 0 {
		switch args[0] {
		case "help", "-h", "--help":
			fmt.Fprint(stdout, daemonUsage)
			return exitOK
		case "version", "-version", "--version":
			fmt.Fprintf(stdout, "bezorgd %s\n", version)
			return exitOK
		}
	}
	fs := flag.NewFlagSet("bezorgd", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, daemonUsage)
		fs.PrintDefaults()
	}
	socket := fs.String("socket", "", "control socket (default BEZORG_SOCKET or a per-user path)")
	state := fs.String("state-dir", "", "job state directory (default BEZORG_STATE_DIR or a per-user path)")
	logLevel := fs.String("log-level", "info", "debug, info, warn, or error")
	logFormat := fs.String("log-format", "text", "text or json")
	maxJobs := fs.Int("max-jobs", 2, "maximum downloads running at once")
	webListen := fs.String("web-listen", strings.TrimSpace(os.Getenv("BEZORG_WEB_LISTEN")), "loopback or wildcard address for the embedded dashboard (for example 127.0.0.1:8080); empty disables it")
	metricsListen := fs.String("metrics-listen", metrics.FromEnv(), metrics.ListenHelp)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "bezorgd: does not take positional arguments")
		return exitUsage
	}
	publicRead, err := webPublicReadEnabled()
	if err != nil {
		fmt.Fprintln(stderr, "bezorgd:", err)
		return exitUsage
	}
	logger, err := newLogger(stderr, *logLevel, *logFormat)
	if err != nil {
		fmt.Fprintln(stderr, "bezorgd:", err)
		return exitUsage
	}
	ctx, cancel := signalContext(context.Background())
	defer cancel()
	err = daemon.Run(ctx, daemon.Options{
		StateDir:      paths.StateDir(*state),
		Socket:        paths.Socket(*socket),
		Logger:        logger,
		Version:       version,
		MaxJobs:       *maxJobs,
		WebListen:     strings.TrimSpace(*webListen),
		WebPublicRead: publicRead,
		MetricsListen: strings.TrimSpace(*metricsListen),
	})
	if err == nil || errors.Is(err, context.Canceled) {
		return exitOK
	}
	fmt.Fprintln(stderr, "bezorgd:", err)
	if errors.Is(err, control.ErrAlreadyRunning) {
		return exitDaemon
	}
	return exitFailed
}

func runForeground(args []string, stdout, stderr io.Writer) int {
	fs := newJobFlagSet("bezorg run", stderr, runUsage)
	metricsListen := fs.String("metrics-listen", metrics.FromEnv(), metrics.ListenHelp)
	cfg, err := finishJobFlags(fs, args)
	if err != nil {
		return usageStatus(err, stderr)
	}
	logger, err := newLogger(stderr, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		fmt.Fprintln(stderr, "bezorg:", err)
		return exitUsage
	}
	ctx, cancel := signalContext(context.Background())
	defer cancel()
	stopMetrics, err := startMetrics(ctx, *metricsListen, logger)
	if err != nil {
		fmt.Fprintln(stderr, "bezorg:", err)
		return exitFailed
	}
	defer stopMetrics()
	result, err := transfer.Run(ctx, transfer.Request{
		Config: cfg,
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
	})
	printResult(stdout, result)
	if errors.Is(err, context.Canceled) {
		return exitCanceled
	}
	if err != nil {
		fmt.Fprintln(stderr, "bezorg:", err)
		return exitFailed
	}
	return exitOK
}

func runFetch(args []string, stdout, stderr io.Writer, wait bool) int {
	_ = wait
	cfg, socket, waitFlag, err := parseFetch(args, stderr)
	if err != nil {
		return usageStatus(err, stderr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := control.NewClient(paths.Socket(socket))
	rec, err := client.Submit(ctx, cfg)
	if err != nil {
		fmt.Fprintln(stderr, "bezorg:", err)
		if errors.Is(err, control.ErrDaemonUnavailable) {
			fmt.Fprintln(stderr, "start the daemon with bezorgd, or use bezorg run for a foreground download")
			return exitDaemon
		}
		return exitFailed
	}
	fmt.Fprintln(stdout, rec.ID)
	if !waitFlag {
		return exitOK
	}
	return waitFor(context.Background(), client, rec.ID, stdout, stderr)
}

func runJobs(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bezorg jobs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, jobsUsage) }
	socket := fs.String("socket", "", "control socket")
	externalID := fs.String("external-id", "", "exact external id to match")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if _, err := config.NormalizeExternalID(*externalID); err != nil {
		fmt.Fprintln(stderr, "bezorg:", err)
		return exitUsage
	}
	client := control.NewClient(paths.Socket(*socket))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	jobs, err := client.List(ctx, strings.TrimSpace(*externalID))
	if err != nil {
		return daemonStatus(err, stderr)
	}
	tw := tabwriter.NewWriter(stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tSTATE\tEXTERNAL\tFILES\tBYTES\tREMOTE")
	for _, rec := range jobs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			rec.ID,
			rec.State,
			rec.ExternalID,
			fmt.Sprintf("%d/%d", rec.Progress.FilesDone, rec.Progress.FilesTotal),
			format.Bytes(rec.Progress.BytesDone),
			rec.Config.Remote,
		)
	}
	_ = tw.Flush()
	return exitOK
}

func runStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bezorg status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, "usage: bezorg status [--socket path] <job>\n") }
	socket := fs.String("socket", "", "control socket")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "bezorg status: expected a job id")
		return exitUsage
	}
	client := control.NewClient(paths.Socket(*socket))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rec, err := client.Get(ctx, fs.Arg(0))
	if err != nil {
		return daemonStatus(err, stderr)
	}
	printJob(stdout, rec)
	return exitOK
}

func runLogs(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bezorg logs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, "usage: bezorg logs [--follow] [--socket path] <job>\n") }
	socket := fs.String("socket", "", "control socket")
	follow := fs.Bool("follow", false, "keep reading until the job finishes")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "bezorg logs: expected a job id")
		return exitUsage
	}
	client := control.NewClient(paths.Socket(*socket))
	after := 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		lines, next, err := client.Logs(ctx, fs.Arg(0), after)
		cancel()
		if err != nil {
			return daemonStatus(err, stderr)
		}
		for _, line := range lines {
			fmt.Fprintln(stdout, line)
		}
		after = next
		if !*follow {
			return exitOK
		}
		ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
		rec, err := client.Get(ctx, fs.Arg(0))
		cancel()
		if err != nil {
			return daemonStatus(err, stderr)
		}
		if job.Terminal(rec.State) && len(lines) == 0 {
			if rec.ExitCode > 0 {
				return rec.ExitCode
			}
			return exitOK
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func runCancel(args []string, stdout, stderr io.Writer) int {
	return simpleJobCommand(args, stderr, stdout, "cancel", func(ctx context.Context, client *control.Client, id string) (job.Record, error) {
		return client.Cancel(ctx, id)
	})
}

func runRetry(args []string, stdout, stderr io.Writer) int {
	return simpleJobCommand(args, stderr, stdout, "retry", func(ctx context.Context, client *control.Client, id string) (job.Record, error) {
		return client.Retry(ctx, id)
	})
}

func runWait(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bezorg wait", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, "usage: bezorg wait [--socket path] <job>\n") }
	socket := fs.String("socket", "", "control socket")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "bezorg wait: expected a job id")
		return exitUsage
	}
	client := control.NewClient(paths.Socket(*socket))
	return waitFor(context.Background(), client, fs.Arg(0), stdout, stderr)
}

func simpleJobCommand(args []string, stderr, stdout io.Writer, name string, fn func(context.Context, *control.Client, string) (job.Record, error)) int {
	fs := flag.NewFlagSet("bezorg "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprintf(stderr, "usage: bezorg %s [--socket path] <job>\n", name) }
	socket := fs.String("socket", "", "control socket")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 1 {
		fmt.Fprintf(stderr, "bezorg %s: expected a job id\n", name)
		return exitUsage
	}
	client := control.NewClient(paths.Socket(*socket))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rec, err := fn(ctx, client, fs.Arg(0))
	if err != nil {
		return daemonStatus(err, stderr)
	}
	fmt.Fprintf(stdout, "%s %s\n", rec.ID, rec.State)
	return exitOK
}

func waitFor(ctx context.Context, client *control.Client, id string, stdout, stderr io.Writer) int {
	for {
		call, cancel := context.WithTimeout(ctx, 15*time.Second)
		rec, err := client.Get(call, id)
		cancel()
		if err != nil {
			return daemonStatus(err, stderr)
		}
		if job.Terminal(rec.State) {
			printJob(stdout, rec)
			switch rec.ExitCode {
			case 0:
				return exitOK
			case exitCanceled:
				return exitCanceled
			default:
				if rec.State == job.StateCanceled || rec.State == job.StateInterrupted {
					return exitCanceled
				}
				return exitFailed
			}
		}
		select {
		case <-ctx.Done():
			return exitCanceled
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func parseFetch(args []string, stderr io.Writer) (config.Config, string, bool, error) {
	fs := newJobFlagSet("bezorg fetch", stderr, fetchUsage)
	wait := fs.Bool("wait", false, "block until the job finishes and use its exit code")
	cfg, err := finishJobFlags(fs, args)
	if err != nil {
		return config.Config{}, "", false, err
	}
	return cfg, socketFrom(fs), *wait, nil
}

func startMetrics(ctx context.Context, address string, logger *slog.Logger) (func(), error) {
	server, err := metrics.Listen(address, logger)
	if err != nil {
		return nil, err
	}
	if server == nil {
		return func() {}, nil
	}
	go func() {
		if serveErr := server.Serve(ctx); serveErr != nil {
			logger.Error("metrics endpoint stopped", "error", serveErr)
		}
	}()
	logger.Info("metrics listening", "address", server.Address())
	return func() { _ = server.Close() }, nil
}

func newJobFlagSet(name string, stderr io.Writer, usage string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, usage)
		fmt.Fprintln(stderr)
		fs.PrintDefaults()
	}
	fs.String("config", "", "YAML config file; flags override it")
	fs.String("protocol", "", "auto, ftp, ftps, ftps-implicit, or sftp")
	fs.String("host", "", "server hostname")
	fs.Int("port", -1, "server port (default depends on protocol)")
	fs.String("user", "", "login name")
	fs.String("password", "", "login password (or BEZORG_PASSWORD)")
	fs.String("private-key", "", "SSH private key path")
	fs.String("key-passphrase", "", "SSH key passphrase (or BEZORG_KEY_PASSPHRASE)")
	fs.String("known-hosts", "", "SSH known_hosts file")
	fs.Bool("insecure", false, "skip TLS and SSH host verification")
	fs.String("remote", "", "remote file or directory")
	fs.String("local", "", "local directory (or file path for a single remote file)")
	fs.Int("concurrency", 0, "simultaneous connections (default 6)")
	fs.Int("large-concurrency", 0, "simultaneous large streams (default 2)")
	fs.Int64("large-min-bytes", 0, "size threshold for the large-file lane")
	fs.Duration("connect-timeout", 0, "TCP and login timeout")
	fs.Duration("stall-timeout", 0, "abort a transfer that stops sending data")
	fs.Duration("keepalive", 0, "NOOP or SSH keepalive interval (0 uses the config default)")
	fs.Duration("command-timeout", 0, "timeout for LIST and other control commands")
	fs.Int("retries", 0, "attempts per file, including the first")
	fs.Duration("retry-base", 0, "first retry delay")
	fs.Duration("retry-max", 0, "maximum retry delay")
	fs.Int("job-retries", -1, "extra full-job retries after a transient failure")
	fs.String("bandwidth", "", "global limit, as bytes per second or a suffix K, M, or G")
	fs.String("checksum", "", "off, auto, or require")
	fs.String("checksum-algo", "", "auto, md5, sha1, or sha256")
	fs.Bool("disable-epsv", false, "use PASV instead of EPSV")
	fs.Bool("force-ipv4", false, "dial IPv4 only")
	fs.Bool("trust-pasv-ip", false, "connect to the IP advertised by PASV")
	fs.Bool("disable-mlsd", false, "list with LIST instead of MLSD")
	fs.Bool("dry-run", false, "list the remote files and do not write")
	fs.String("log-level", "", "debug, info, warn, or error")
	fs.String("log-format", "", "text or json")
	fs.String("external-id", "", "optional correlation id stored on the job and copied into the completion manifest")
	fs.String("done-suffix", "", "completion manifest suffix appended to the local path (default .bezorg-done.json, or BEZORG_DONE_SUFFIX)")
	fs.String("socket", "", "daemon socket, for fetch")
	return fs
}

func finishJobFlags(fs *flag.FlagSet, args []string) (config.Config, error) {
	if err := fs.Parse(args); err != nil {
		return config.Config{}, err
	}
	if fs.NArg() != 0 {
		return config.Config{}, errors.New("unexpected positional arguments")
	}
	cfg := config.Defaults()
	if path := fs.Lookup("config").Value.String(); path != "" && flagWasSet(fs, "config") {
		loaded, err := config.Load(path)
		if err != nil {
			return config.Config{}, err
		}
		cfg = loaded
	}
	if err := applyFlags(&cfg, fs); err != nil {
		return config.Config{}, err
	}
	if cfg.Password == "" {
		if value := os.Getenv("BEZORG_PASSWORD"); value != "" {
			cfg.Password = value
		}
	}
	if cfg.KeyPassphrase == "" {
		if value := os.Getenv("BEZORG_KEY_PASSPHRASE"); value != "" {
			cfg.KeyPassphrase = value
		}
	}
	return config.Validate(cfg)
}

func applyFlags(cfg *config.Config, fs *flag.FlagSet) error {
	var err error
	fs.Visit(func(f *flag.Flag) {
		if err != nil {
			return
		}
		switch f.Name {
		case "protocol":
			cfg.Protocol = f.Value.String()
		case "host":
			cfg.Host = f.Value.String()
		case "port":
			cfg.Port, _ = strconv.Atoi(f.Value.String())
		case "user":
			cfg.User = f.Value.String()
		case "password":
			cfg.Password = f.Value.String()
		case "private-key":
			cfg.PrivateKey = f.Value.String()
		case "key-passphrase":
			cfg.KeyPassphrase = f.Value.String()
		case "known-hosts":
			cfg.KnownHosts = f.Value.String()
		case "insecure":
			cfg.Insecure = f.Value.String() == "true"
		case "remote":
			cfg.Remote = f.Value.String()
		case "local":
			cfg.Local = f.Value.String()
		case "concurrency":
			cfg.Concurrency, _ = strconv.Atoi(f.Value.String())
		case "large-concurrency":
			cfg.LargeConcurrency, _ = strconv.Atoi(f.Value.String())
		case "large-min-bytes":
			cfg.LargeMinBytes, _ = strconv.ParseInt(f.Value.String(), 10, 64)
		case "connect-timeout":
			cfg.ConnectTimeout = mustDuration(f.Value.String())
		case "stall-timeout":
			cfg.StallTimeout = mustDuration(f.Value.String())
		case "keepalive":
			cfg.Keepalive = mustDuration(f.Value.String())
		case "command-timeout":
			cfg.CommandTimeout = mustDuration(f.Value.String())
		case "retries":
			cfg.Retries, _ = strconv.Atoi(f.Value.String())
		case "retry-base":
			cfg.RetryBase = mustDuration(f.Value.String())
		case "retry-max":
			cfg.RetryMax = mustDuration(f.Value.String())
		case "job-retries":
			cfg.JobRetries, _ = strconv.Atoi(f.Value.String())
		case "bandwidth":
			cfg.Bandwidth, err = parseRate(f.Value.String())
		case "checksum":
			cfg.Checksum = f.Value.String()
		case "checksum-algo":
			cfg.ChecksumAlgo = f.Value.String()
		case "disable-epsv":
			cfg.DisableEPSV = f.Value.String() == "true"
		case "force-ipv4":
			cfg.ForceIPv4 = f.Value.String() == "true"
		case "trust-pasv-ip":
			cfg.TrustPASVIP = f.Value.String() == "true"
		case "disable-mlsd":
			cfg.DisableMLSD = f.Value.String() == "true"
		case "dry-run":
			cfg.DryRun = f.Value.String() == "true"
		case "log-level":
			cfg.LogLevel = f.Value.String()
		case "log-format":
			cfg.LogFormat = f.Value.String()
		case "external-id":
			cfg.ExternalID = f.Value.String()
		case "done-suffix":
			cfg.DoneSuffix = f.Value.String()
		}
	})
	return err
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

func socketFrom(fs *flag.FlagSet) string {
	if fs.Lookup("socket") == nil || !flagWasSet(fs, "socket") {
		return ""
	}
	return fs.Lookup("socket").Value.String()
}

func mustDuration(value string) config.Duration {
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return config.Duration(parsed)
}

func parseRate(value string) (int64, error) {
	value = strings.TrimSpace(strings.ToUpper(value))
	if value == "" || value == "0" {
		return 0, nil
	}
	mult := 1.0
	switch {
	case strings.HasSuffix(value, "G"):
		mult = 1024 * 1024 * 1024
		value = strings.TrimSuffix(value, "G")
	case strings.HasSuffix(value, "M"):
		mult = 1024 * 1024
		value = strings.TrimSuffix(value, "M")
	case strings.HasSuffix(value, "K"):
		mult = 1024
		value = strings.TrimSuffix(value, "K")
	}
	number, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil || number < 0 {
		return 0, fmt.Errorf("bandwidth %q is not a byte rate", value)
	}
	return int64(number * mult), nil
}

func newLogger(w io.Writer, level, formatName string) (*slog.Logger, error) {
	var slogLevel slog.Level
	switch strings.ToLower(level) {
	case "", "info":
		slogLevel = slog.LevelInfo
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		return nil, fmt.Errorf("log level %q is not debug, info, warn, or error", level)
	}
	options := &slog.HandlerOptions{Level: slogLevel}
	switch strings.ToLower(formatName) {
	case "", "text":
		return slog.New(slog.NewTextHandler(w, options)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(w, options)), nil
	default:
		return nil, fmt.Errorf("log format %q is not text or json", formatName)
	}
}

func printResult(w io.Writer, result transfer.Result) {
	if result.FilesTotal == 0 && result.Phase == "" {
		return
	}
	fmt.Fprintf(w, "files %d/%d skipped %d failed %d bytes %s\n",
		result.FilesDone, result.FilesTotal, result.FilesSkipped, result.FilesFailed, format.Bytes(result.BytesDone))
	if result.Manifest != "" {
		fmt.Fprintf(w, "manifest %s\n", result.Manifest)
	}
}

func printJob(w io.Writer, rec job.Record) {
	fmt.Fprintf(w, "job %s\nstate %s\nhost %s\nremote %s\nlocal %s\nfiles %d/%d skipped %d failed %d\nbytes %s / %s\n",
		rec.ID,
		rec.State,
		rec.Config.Host,
		rec.Config.Remote,
		rec.Config.Local,
		rec.Progress.FilesDone,
		rec.Progress.FilesTotal,
		rec.Progress.FilesSkipped,
		rec.Progress.FilesFailed,
		format.Bytes(rec.Progress.BytesDone),
		format.Bytes(rec.Progress.BytesTotal),
	)
	if rec.ExternalID != "" {
		fmt.Fprintf(w, "external_id %s\n", rec.ExternalID)
	}
	if rec.Error != "" {
		fmt.Fprintf(w, "error %s\n", rec.Error)
	}
}

func usageStatus(err error, stderr io.Writer) int {
	if errors.Is(err, flag.ErrHelp) {
		return exitOK
	}
	fmt.Fprintln(stderr, "bezorg:", err)
	return exitUsage
}

func daemonStatus(err error, stderr io.Writer) int {
	fmt.Fprintln(stderr, "bezorg:", err)
	if errors.Is(err, control.ErrDaemonUnavailable) {
		return exitDaemon
	}
	return exitFailed
}

func webPublicReadEnabled() (bool, error) {
	value := strings.TrimSpace(os.Getenv("BEZORG_WEB_PUBLIC_READ"))
	if value == "" {
		return false, nil
	}
	enabled, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("BEZORG_WEB_PUBLIC_READ %q is not true or false", value)
	}
	return enabled, nil
}

func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			signal.Stop(signals)
			cancel()
		case <-ctx.Done():
			signal.Stop(signals)
		}
	}()
	return ctx, cancel
}

const clientUsage = `bezorg downloads large FTP, FTPS, and SFTP deliveries.

Usage:
  bezorg run [flags]              Download in the foreground
  bezorg fetch [flags]            Queue a download with bezorgd
  bezorg jobs                     List daemon jobs
  bezorg status <job>             Show one job
  bezorg logs [--follow] <job>    Show job logs
  bezorg wait <job>               Block until a job finishes
  bezorg cancel <job>             Stop a job
  bezorg retry <job>              Queue a failed job again
  bezorg version

Run bezorg <command> -h for flags. A YAML file (--config) supplies the same
settings; command flags override the file. Passwords may come from
BEZORG_PASSWORD. Start bezorgd before fetch so downloads survive logout.

`

const runUsage = `usage: bezorg run [flags]

Download in the foreground. The process exits when the delivery finishes.
Partial files are kept and a later run resumes them.
--metrics-listen serves Prometheus metrics for this process. It is also read
from BEZORG_METRICS_LISTEN and stays off when that is empty or off.

`

const fetchUsage = `usage: bezorg fetch [flags]

Queue a download on the local bezorgd daemon and print the job id.
The daemon keeps running after this command exits. --wait blocks until
the job reaches a terminal state and uses the job's exit code.

`

const jobsUsage = `usage: bezorg jobs [--socket path] [--external-id id]

--external-id keeps only jobs whose external id matches exactly.

`

const daemonUsage = `bezorgd runs downloads in the background.

Usage:
  bezorgd [--socket path] [--state-dir path] [--log-level level] [--log-format text|json] [--max-jobs n] [--web-listen host:port] [--metrics-listen host:port]

Jobs are submitted with bezorg fetch. On startup, queued, running, and
interrupted jobs resume from the partial files already on disk.
--web-listen serves the embedded dashboard. It is also read from
BEZORG_WEB_LISTEN and stays off when that is empty. BEZORG_WEB_PUBLIC_READ=true
allows browsing without a token; cancel and retry still require one.
--metrics-listen serves Prometheus metrics at /metrics. It is also read from
BEZORG_METRICS_LISTEN and stays off when that is empty or off. The bind rules
match --web-listen: loopback or a wildcard.

`
