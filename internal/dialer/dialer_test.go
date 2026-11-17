package dialer

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
)

func TestResolveProtocolFromBanner(t *testing.T) {
	cases := []struct {
		banner string
		want   string
	}{
		{"220 lab FTP ready\r\n", config.ProtocolFTP},
		{"SSH-2.0-test\r\n", config.ProtocolSFTP},
	}
	for _, tc := range cases {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			_, _ = conn.Write([]byte(tc.banner))
		}()

		host, portText, err := net.SplitHostPort(ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		port, _ := strconv.Atoi(portText)
		cfg := config.Defaults()
		cfg.Host = host
		cfg.Port = port
		cfg.Remote = "/drop"
		cfg.Local = t.TempDir()
		cfg.ConnectTimeout = config.Duration(2 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		got, err := ResolveProtocol(ctx, cfg)
		cancel()
		ln.Close()
		<-done
		if err != nil {
			t.Fatalf("banner %q: %v", tc.banner, err)
		}
		if got.Protocol != tc.want || got.Port != port {
			t.Fatalf("banner %q: protocol %s port %d", tc.banner, got.Protocol, got.Port)
		}
	}
}

func TestImplicitFTPSSkipsBanner(t *testing.T) {
	cfg := config.Defaults()
	cfg.Protocol = config.ProtocolAuto
	cfg.Host = "127.0.0.1"
	cfg.Port = 990
	cfg.Remote = "/drop"
	cfg.DryRun = true
	got, err := ResolveProtocol(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Protocol != config.ProtocolFTPSImplicit {
		t.Fatalf("protocol = %s", got.Protocol)
	}
}
