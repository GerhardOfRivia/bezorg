// Package config loads bezorg job settings from YAML, flags, and the environment.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	ProtocolAuto         = "auto"
	ProtocolFTP          = "ftp"
	ProtocolFTPS         = "ftps"
	ProtocolFTPSImplicit = "ftps-implicit"
	ProtocolSFTP         = "sftp"

	ChecksumOff     = "off"
	ChecksumAuto    = "auto"
	ChecksumRequire = "require"

	AlgoAuto   = "auto"
	AlgoMD5    = "md5"
	AlgoSHA1   = "sha1"
	AlgoSHA256 = "sha256"

	tempSuffixDefault = ".bezorg-partial"

	// MaxExternalIDBytes is the maximum stored length of an external id.
	MaxExternalIDBytes = 256
)

// Duration is a time.Duration that accepts strings such as "30s" in YAML and JSON.
type Duration time.Duration

// Std returns the standard-library duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// MarshalYAML encodes the duration as a string.
func (d Duration) MarshalYAML() (any, error) {
	return d.Std().String(), nil
}

// UnmarshalYAML parses a duration string.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var raw string
	if err := value.Decode(&raw); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", raw, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalJSON encodes the duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Std().String())
}

// UnmarshalJSON accepts a duration string or a nanosecond count.
func (d *Duration) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	var raw string
	if err := json.Unmarshal(b, &raw); err == nil {
		parsed, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("parse duration %q: %w", raw, err)
		}
		*d = Duration(parsed)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*d = Duration(n)
	return nil
}

// Config is one download job. Zero values are not valid; start from Defaults.
type Config struct {
	Protocol      string `json:"protocol" yaml:"protocol"`
	Host          string `json:"host" yaml:"host"`
	Port          int    `json:"port" yaml:"port"`
	User          string `json:"user" yaml:"user"`
	Password      string `json:"password,omitempty" yaml:"password,omitempty"`
	PrivateKey    string `json:"private_key,omitempty" yaml:"private_key,omitempty"`
	KeyPassphrase string `json:"key_passphrase,omitempty" yaml:"key_passphrase,omitempty"`
	KnownHosts    string `json:"known_hosts,omitempty" yaml:"known_hosts,omitempty"`
	Insecure      bool   `json:"insecure" yaml:"insecure"`

	Remote string `json:"remote" yaml:"remote"`
	Local  string `json:"local" yaml:"local"`

	Concurrency      int   `json:"concurrency" yaml:"concurrency"`
	LargeConcurrency int   `json:"large_concurrency" yaml:"large_concurrency"`
	LargeMinBytes    int64 `json:"large_min_bytes" yaml:"large_min_bytes"`

	ConnectTimeout    Duration `json:"connect_timeout" yaml:"connect_timeout"`
	StallTimeout      Duration `json:"stall_timeout" yaml:"stall_timeout"`
	Keepalive         Duration `json:"keepalive" yaml:"keepalive"`
	KeepaliveMinBytes int64    `json:"keepalive_min_bytes" yaml:"keepalive_min_bytes"`
	CommandTimeout    Duration `json:"command_timeout" yaml:"command_timeout"`

	Retries   int      `json:"retries" yaml:"retries"`
	RetryBase Duration `json:"retry_base" yaml:"retry_base"`
	RetryMax  Duration `json:"retry_max" yaml:"retry_max"`

	JobRetries   int      `json:"job_retries" yaml:"job_retries"`
	JobRetryBase Duration `json:"job_retry_base" yaml:"job_retry_base"`
	JobRetryMax  Duration `json:"job_retry_max" yaml:"job_retry_max"`

	Bandwidth     int64  `json:"bandwidth" yaml:"bandwidth"`
	Checksum      string `json:"checksum" yaml:"checksum"`
	ChecksumAlgo  string `json:"checksum_algo" yaml:"checksum_algo"`
	ProbeZeroSize bool   `json:"probe_zero_size" yaml:"probe_zero_size"`

	Passive     bool `json:"passive" yaml:"passive"`
	DisableEPSV bool `json:"disable_epsv" yaml:"disable_epsv"`
	ForceIPv4   bool `json:"force_ipv4" yaml:"force_ipv4"`
	TrustPASVIP bool `json:"trust_pasv_ip" yaml:"trust_pasv_ip"`
	DisableMLSD bool `json:"disable_mlsd" yaml:"disable_mlsd"`
	DryRun      bool `json:"dry_run" yaml:"dry_run"`

	TempSuffix string `json:"temp_suffix" yaml:"temp_suffix"`
	LogLevel   string `json:"log_level" yaml:"log_level"`
	LogFormat  string `json:"log_format" yaml:"log_format"`

	// ExternalID is an optional caller-defined correlation id. Empty means unset.
	// The daemon copies it onto the job record; the transfer engine writes it
	// into a sibling sidecar before the final rename.
	ExternalID string `json:"external_id,omitempty" yaml:"external_id,omitempty"`
}

// Defaults returns production defaults for a mixed imagery drop.
func Defaults() Config {
	return Config{
		Protocol:          ProtocolAuto,
		User:              "anonymous",
		Concurrency:       6,
		LargeConcurrency:  0,
		LargeMinBytes:     64 << 20,
		ConnectTimeout:    Duration(30 * time.Second),
		StallTimeout:      Duration(2 * time.Minute),
		Keepalive:         Duration(45 * time.Second),
		KeepaliveMinBytes: 32 << 20,
		CommandTimeout:    Duration(2 * time.Minute),
		Retries:           8,
		RetryBase:         Duration(time.Second),
		RetryMax:          Duration(2 * time.Minute),
		JobRetries:        3,
		JobRetryBase:      Duration(30 * time.Second),
		JobRetryMax:       Duration(10 * time.Minute),
		Checksum:          ChecksumAuto,
		ChecksumAlgo:      AlgoAuto,
		ProbeZeroSize:     true,
		Passive:           true,
		TempSuffix:        tempSuffixDefault,
		LogLevel:          "info",
		LogFormat:         "text",
	}
}

// Load reads a YAML config file and fills any omitted field from Defaults.
// ${VAR} and $VAR in string values expand from the environment.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: read %s: %w", path, err)
	}
	cfg := Defaults()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("config: parse %s: %w", path, err)
	}
	cfg.expandEnv()
	return cfg, nil
}

// Validate checks cfg and fills derived defaults (port, large concurrency,
// anonymous password). It returns the normalized copy.
func Validate(cfg Config) (Config, error) {
	cfg.Protocol = strings.ToLower(strings.TrimSpace(cfg.Protocol))
	cfg.Checksum = strings.ToLower(strings.TrimSpace(cfg.Checksum))
	cfg.ChecksumAlgo = strings.ToLower(strings.TrimSpace(cfg.ChecksumAlgo))
	cfg.LogLevel = strings.ToLower(strings.TrimSpace(cfg.LogLevel))
	cfg.LogFormat = strings.ToLower(strings.TrimSpace(cfg.LogFormat))
	cfg.Host = strings.TrimSpace(cfg.Host)
	cfg.User = strings.TrimSpace(cfg.User)
	cfg.Remote = strings.TrimSpace(cfg.Remote)
	cfg.Local = strings.TrimSpace(cfg.Local)
	externalID, err := NormalizeExternalID(cfg.ExternalID)
	if err != nil {
		return Config{}, err
	}
	cfg.ExternalID = externalID

	switch cfg.Protocol {
	case ProtocolAuto, ProtocolFTP, ProtocolFTPS, ProtocolFTPSImplicit, ProtocolSFTP:
	default:
		return Config{}, fmt.Errorf("config: protocol %q is not auto, ftp, ftps, ftps-implicit, or sftp", cfg.Protocol)
	}
	if cfg.Host == "" {
		return Config{}, errors.New("config: host is required")
	}
	if strings.ContainsAny(cfg.Host, " \t\r\n") {
		return Config{}, errors.New("config: host contains whitespace")
	}
	if cfg.Port < 0 || cfg.Port > 65535 {
		return Config{}, fmt.Errorf("config: port %d is out of range", cfg.Port)
	}
	if cfg.User == "" {
		return Config{}, errors.New("config: user is required")
	}
	if strings.ContainsAny(cfg.Password, "\r\n") || strings.ContainsAny(cfg.User, "\r\n") {
		return Config{}, errors.New("config: user and password cannot contain newlines")
	}
	if cfg.Password == "" && cfg.PrivateKey == "" {
		if strings.EqualFold(cfg.User, "anonymous") {
			cfg.Password = "anonymous"
		} else if cfg.Protocol == ProtocolSFTP {
			return Config{}, errors.New("config: sftp requires a password or private_key")
		} else {
			return Config{}, errors.New("config: password is required")
		}
	}
	if cfg.Remote == "" {
		return Config{}, errors.New("config: remote path is required")
	}
	if strings.ContainsAny(cfg.Remote, "\r\n") {
		return Config{}, errors.New("config: remote path cannot contain newlines")
	}
	if cfg.Local == "" && !cfg.DryRun {
		return Config{}, errors.New("config: local path is required")
	}
	if cfg.Concurrency < 1 {
		return Config{}, errors.New("config: concurrency must be at least 1")
	}
	if cfg.Concurrency > 64 {
		return Config{}, errors.New("config: concurrency above 64 is refused; raise the server limit first")
	}
	if cfg.LargeConcurrency < 0 || cfg.LargeConcurrency > cfg.Concurrency {
		return Config{}, errors.New("config: large_concurrency must be between 0 and concurrency")
	}
	if cfg.LargeConcurrency == 0 {
		cfg.LargeConcurrency = defaultLarge(cfg.Concurrency)
	}
	if cfg.LargeMinBytes < 1 {
		return Config{}, errors.New("config: large_min_bytes must be positive")
	}
	if cfg.ConnectTimeout.Std() <= 0 || cfg.StallTimeout.Std() <= 0 || cfg.CommandTimeout.Std() <= 0 {
		return Config{}, errors.New("config: connect, stall, and command timeouts must be positive")
	}
	if cfg.Keepalive.Std() < 0 {
		return Config{}, errors.New("config: keepalive cannot be negative")
	}
	if cfg.KeepaliveMinBytes <= 0 {
		cfg.KeepaliveMinBytes = 32 << 20
	}
	if cfg.Retries < 1 {
		return Config{}, errors.New("config: retries must be at least 1")
	}
	if cfg.RetryBase.Std() <= 0 || cfg.RetryMax.Std() < cfg.RetryBase.Std() {
		return Config{}, errors.New("config: retry_max must be greater than or equal to retry_base")
	}
	if cfg.JobRetries < 0 {
		return Config{}, errors.New("config: job_retries cannot be negative")
	}
	if cfg.JobRetryBase.Std() <= 0 || cfg.JobRetryMax.Std() < cfg.JobRetryBase.Std() {
		return Config{}, errors.New("config: job_retry_max must be greater than or equal to job_retry_base")
	}
	if cfg.Bandwidth < 0 {
		return Config{}, errors.New("config: bandwidth cannot be negative")
	}
	switch cfg.Checksum {
	case ChecksumOff, ChecksumAuto, ChecksumRequire:
	default:
		return Config{}, fmt.Errorf("config: checksum %q is not off, auto, or require", cfg.Checksum)
	}
	switch cfg.ChecksumAlgo {
	case AlgoAuto, AlgoMD5, AlgoSHA1, AlgoSHA256:
	default:
		return Config{}, fmt.Errorf("config: checksum_algo %q is not auto, md5, sha1, or sha256", cfg.ChecksumAlgo)
	}
	if !cfg.Passive {
		return Config{}, errors.New("config: only passive mode is supported")
	}
	if cfg.TempSuffix == "" || strings.ContainsAny(cfg.TempSuffix, `/\`) {
		return Config{}, errors.New("config: temp_suffix must be a non-empty filename suffix")
	}
	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return Config{}, fmt.Errorf("config: log_level %q is not debug, info, warn, or error", cfg.LogLevel)
	}
	switch cfg.LogFormat {
	case "text", "json":
	default:
		return Config{}, fmt.Errorf("config: log_format %q is not text or json", cfg.LogFormat)
	}
	if cfg.PrivateKey != "" {
		cfg.PrivateKey = expandPath(cfg.PrivateKey)
	}
	if cfg.KnownHosts != "" {
		cfg.KnownHosts = expandPath(cfg.KnownHosts)
	}
	if cfg.Local != "" && !cfg.DryRun {
		abs, err := filepath.Abs(cfg.Local)
		if err != nil {
			return Config{}, fmt.Errorf("config: local path: %w", err)
		}
		cfg.Local = abs
	}
	return cfg, nil
}

// NormalizeExternalID trims an optional correlation id. An empty result is
// unset. The id is at most MaxExternalIDBytes and cannot contain a NUL.
func NormalizeExternalID(id string) (string, error) {
	id = strings.TrimSpace(id)
	if strings.ContainsRune(id, 0) {
		return "", errors.New("config: external_id cannot contain NUL")
	}
	if len(id) > MaxExternalIDBytes {
		return "", fmt.Errorf("config: external_id is longer than %d bytes", MaxExternalIDBytes)
	}
	return id, nil
}

// Redacted returns a copy safe to print. Secrets are cleared.
func (cfg Config) Redacted() Config {
	out := cfg
	if out.Password != "" {
		out.Password = "****"
	}
	if out.KeyPassphrase != "" {
		out.KeyPassphrase = "****"
	}
	return out
}

// Address is host:port using the protocol default when port is unset.
func (cfg Config) Address(protocol string) string {
	port := cfg.Port
	if port == 0 {
		port = DefaultPort(protocol)
	}
	return net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", port))
}

// DefaultPort returns the conventional port for a resolved protocol.
func DefaultPort(protocol string) int {
	switch protocol {
	case ProtocolSFTP:
		return 22
	case ProtocolFTPSImplicit:
		return 990
	default:
		return 21
	}
}

// EffectiveLarge is the large-file stream cap after validation.
func (cfg Config) EffectiveLarge() int {
	if cfg.LargeConcurrency > 0 {
		return cfg.LargeConcurrency
	}
	return defaultLarge(cfg.Concurrency)
}

func defaultLarge(concurrency int) int {
	if concurrency <= 1 {
		return 1
	}
	if concurrency-1 < 2 {
		return concurrency - 1
	}
	return 2
}

func (cfg *Config) expandEnv() {
	cfg.Protocol = expand(cfg.Protocol)
	cfg.Host = expand(cfg.Host)
	cfg.User = expand(cfg.User)
	cfg.Password = expand(cfg.Password)
	cfg.PrivateKey = expand(cfg.PrivateKey)
	cfg.KeyPassphrase = expand(cfg.KeyPassphrase)
	cfg.KnownHosts = expand(cfg.KnownHosts)
	cfg.Remote = expand(cfg.Remote)
	cfg.Local = expand(cfg.Local)
	cfg.TempSuffix = expand(cfg.TempSuffix)
	cfg.Checksum = expand(cfg.Checksum)
	cfg.ChecksumAlgo = expand(cfg.ChecksumAlgo)
	cfg.LogLevel = expand(cfg.LogLevel)
	cfg.LogFormat = expand(cfg.LogFormat)
}

func expand(value string) string {
	return os.Expand(value, func(key string) string {
		return os.Getenv(key)
	})
}

func expandPath(path string) string {
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err == nil && home != "" {
			if path == "~" {
				return home
			}
			return filepath.Join(home, path[2:])
		}
	}
	return path
}
