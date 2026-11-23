package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestHelpAndUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := Run([]string{"help"}, &stdout, &stderr, "test"); code != exitOK {
		t.Fatalf("help code %d stderr %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "bezorg fetch") {
		t.Fatalf("help = %s", stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"run", "--host", "example.test"}, &stdout, &stderr, "test"); code != exitUsage {
		t.Fatalf("incomplete run code %d stderr %s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := RunDaemon([]string{"--help"}, &stdout, &stderr, "test"); code != exitOK {
		t.Fatalf("daemon help code %d", code)
	}
	if !strings.Contains(stderr.String(), "--web-listen") && !strings.Contains(stdout.String(), "--web-listen") {
		t.Fatalf("daemon help missing web listen\nstdout %s\nstderr %s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "--metrics-listen") && !strings.Contains(stdout.String(), "--metrics-listen") {
		t.Fatalf("daemon help missing metrics listen\nstdout %s\nstderr %s", stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"run", "-h"}, &stdout, &stderr, "test"); code != exitOK {
		t.Fatalf("run help code %d stderr %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--metrics-listen") && !strings.Contains(stdout.String(), "--metrics-listen") {
		t.Fatalf("run help missing metrics listen\nstdout %s\nstderr %s", stdout.String(), stderr.String())
	}
}

func TestWebPublicReadRejectsInvalid(t *testing.T) {
	t.Setenv("BEZORG_WEB_PUBLIC_READ", "maybe")
	var stdout, stderr bytes.Buffer
	code := RunDaemon(nil, &stdout, &stderr, "test")
	if code != exitUsage || !strings.Contains(stderr.String(), "BEZORG_WEB_PUBLIC_READ") {
		t.Fatalf("code %d stderr %s", code, stderr.String())
	}
}

func TestExternalIDFlagIsValidated(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"jobs", "--external-id", strings.Repeat("x", 257)}, &stdout, &stderr, "test")
	if code != exitUsage || !strings.Contains(stderr.String(), "external_id") {
		t.Fatalf("code %d stderr %s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	code = Run([]string{"run", "--external-id", "bad\x00id", "--host", "example.test"}, &stdout, &stderr, "test")
	if code != exitUsage || !strings.Contains(stderr.String(), "external_id") {
		t.Fatalf("run code %d stderr %s", code, stderr.String())
	}
}

func TestParseRate(t *testing.T) {
	got, err := parseRate("2M")
	if err != nil || got != 2*1024*1024 {
		t.Fatalf("rate = %d %v", got, err)
	}
}
