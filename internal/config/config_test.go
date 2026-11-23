package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsAndEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job.yaml")
	body := []byte("host: ftp.example.com\nremote: /drop\nlocal: " + filepath.Join(dir, "out") + "\npassword: ${BEZORG_PASSWORD}\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BEZORG_PASSWORD", "s3cret")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err = Validate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Password != "s3cret" {
		t.Fatalf("password = %q", cfg.Password)
	}
	if cfg.Concurrency != 6 {
		t.Fatalf("concurrency = %d", cfg.Concurrency)
	}
	if cfg.LargeConcurrency != 2 {
		t.Fatalf("large concurrency = %d", cfg.LargeConcurrency)
	}
	if cfg.Protocol != ProtocolAuto {
		t.Fatalf("protocol = %s", cfg.Protocol)
	}
	if cfg.ConnectTimeout.Std() != 30*time.Second {
		t.Fatalf("connect timeout = %s", cfg.ConnectTimeout.Std())
	}
	redacted := cfg.Redacted()
	if redacted.Password != "****" {
		t.Fatalf("redacted password = %q", redacted.Password)
	}
}

func TestRejectsActiveModeAndBadProtocol(t *testing.T) {
	cfg := Defaults()
	cfg.Host = "ftp.example.com"
	cfg.Remote = "/drop"
	cfg.Local = t.TempDir()
	cfg.Passive = false
	if _, err := Validate(cfg); err == nil {
		t.Fatal("expected passive=false to fail")
	}
	cfg.Passive = true
	cfg.Protocol = "scp"
	if _, err := Validate(cfg); err == nil {
		t.Fatal("expected bad protocol to fail")
	}
}

func TestExternalIDIsTrimmedAndBounded(t *testing.T) {
	cfg := Defaults()
	cfg.Host = "ftp.example.com"
	cfg.Remote = "/drop"
	cfg.Local = t.TempDir()
	cfg.Password = "secret"
	cfg.ExternalID = "  batch-42  "
	got, err := Validate(cfg)
	if err != nil || got.ExternalID != "batch-42" {
		t.Fatalf("external id = %q %v", got.ExternalID, err)
	}
	cfg.ExternalID = strings.Repeat("x", MaxExternalIDBytes+1)
	if _, err := Validate(cfg); err == nil {
		t.Fatal("oversized external id accepted")
	}
	cfg.ExternalID = "bad\x00id"
	if _, err := Validate(cfg); err == nil {
		t.Fatal("external id with NUL accepted")
	}
}

func TestDurationRoundTrip(t *testing.T) {
	var d Duration
	if err := d.UnmarshalJSON([]byte(`"45s"`)); err != nil {
		t.Fatal(err)
	}
	if d.Std() != 45*time.Second {
		t.Fatalf("duration = %s", d.Std())
	}
	raw, err := d.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `"45s"` {
		t.Fatalf("json = %s", raw)
	}
}
