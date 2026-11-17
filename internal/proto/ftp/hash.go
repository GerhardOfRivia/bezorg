package ftp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/textproto"
	"strings"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
	"github.com/GerhardOfRivia/bezorg/internal/remote"
)

// NewChecksummer opens a dedicated control connection for HASH / MD5 / SHA
// commands. When the server advertises none of them, the returned checksummer
// reports that support is absent and does not keep a connection open.
func NewChecksummer(ctx context.Context, cfg config.Config, logger *slog.Logger) (remote.Checksummer, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	protocol := cfg.Protocol
	if protocol == "" || protocol == config.ProtocolAuto {
		protocol = config.ProtocolFTP
	}
	if protocol == config.ProtocolSFTP {
		return noopSum{}, nil
	}
	client, err := dialCommand(ctx, cfg, protocol)
	if err != nil {
		return nil, err
	}
	if !client.supported() {
		_ = client.Close()
		logger.Info("server does not advertise a checksum command")
		return noopSum{}, nil
	}
	return client, nil
}

type noopSum struct{}

func (noopSum) Sum(context.Context, string, string) (string, bool, error) {
	return "", false, nil
}

func (noopSum) Close() error { return nil }

type hashClient struct {
	conn    net.Conn
	text    *textproto.Conn
	algos   map[string]bool
	hasHASH bool
}

func dialCommand(ctx context.Context, cfg config.Config, protocol string) (*hashClient, error) {
	if cfg.Port == 0 {
		cfg.Port = config.DefaultPort(protocol)
	}
	dialer := &net.Dialer{Timeout: cfg.ConnectTimeout.Std(), KeepAlive: 30 * time.Second}
	network := "tcp"
	if cfg.ForceIPv4 {
		network = "tcp4"
	}
	raw, err := dialer.DialContext(ctx, network, cfg.Address(protocol))
	if err != nil {
		return nil, fmt.Errorf("ftp: checksum dial: %w", err)
	}
	conn := net.Conn(raw)
	tlsConfig := &tls.Config{
		ServerName:         cfg.Host,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.Insecure,
		ClientSessionCache: tls.NewLRUClientSessionCache(4),
	}
	if protocol == config.ProtocolFTPSImplicit {
		tlsConn := tls.Client(raw, tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, err
		}
		conn = tlsConn
	}
	_ = conn.SetDeadline(time.Now().Add(cfg.CommandTimeout.Std()))
	defer conn.SetDeadline(time.Time{})
	text := textproto.NewConn(conn)
	if _, _, err := text.ReadResponse(220); err != nil {
		_ = text.Close()
		return nil, fmt.Errorf("ftp: checksum banner: %w", err)
	}
	if protocol == config.ProtocolFTPS {
		if err := cmdOK(text, 234, "AUTH TLS"); err != nil {
			_ = text.Close()
			return nil, err
		}
		tlsConn := tls.Client(conn, tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = text.Close()
			return nil, err
		}
		conn = tlsConn
		text = textproto.NewConn(bufConn{Conn: conn})
	}
	if err := login(text, cfg.User, cfg.Password); err != nil {
		_ = text.Close()
		return nil, err
	}
	if protocol != config.ProtocolFTP {
		if err := cmdOK(text, 200, "PBSZ 0"); err != nil {
			_ = text.Close()
			return nil, err
		}
		if err := cmdOK(text, 200, "PROT P"); err != nil {
			_ = text.Close()
			return nil, err
		}
	}
	features, err := feat(text)
	if err != nil {
		_ = text.Close()
		return nil, err
	}
	_ = cmdOK(text, 200, "TYPE I")
	client := &hashClient{conn: conn, text: text, algos: map[string]bool{}}
	if desc, ok := features["HASH"]; ok {
		client.hasHASH = true
		upper := strings.ToUpper(desc)
		if strings.Contains(upper, "SHA-256") || strings.Contains(upper, "SHA256") {
			client.algos["sha256"] = true
		}
		if strings.Contains(upper, "SHA-1") || strings.Contains(upper, "SHA1") {
			client.algos["sha1"] = true
		}
		if strings.Contains(upper, "MD5") {
			client.algos["md5"] = true
		}
		if len(client.algos) == 0 {
			client.algos["sha256"] = true
		}
	}
	if _, ok := features["XSHA256"]; ok {
		client.algos["sha256"] = true
	}
	if _, ok := features["XSHA1"]; ok {
		client.algos["sha1"] = true
	}
	if _, ok := features["XMD5"]; ok || features["MD5"] != "" || hasKey(features, "MD5") {
		client.algos["md5"] = true
	}
	return client, nil
}

func (h *hashClient) supported() bool {
	return h != nil && (h.hasHASH || len(h.algos) > 0)
}

func (h *hashClient) Sum(ctx context.Context, remotePath, algo string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	chosen := chooseAlgo(h.algos, algo)
	if chosen == "" {
		return "", false, nil
	}
	if h.hasHASH {
		hashName := map[string]string{"sha256": "SHA-256", "sha1": "SHA-1", "md5": "MD5"}[chosen]
		_, _, _ = command(h.text, "OPTS HASH "+hashName)
		code, line, err := command(h.text, "HASH "+remotePath)
		if err == nil && code >= 200 && code < 300 {
			if sum := extractHex(line); sum != "" {
				return sum, true, nil
			}
		}
	}
	var cmds []string
	switch chosen {
	case "sha256":
		cmds = []string{"XSHA256 " + remotePath}
	case "sha1":
		cmds = []string{"XSHA1 " + remotePath}
	case "md5":
		cmds = []string{"XMD5 " + remotePath, "MD5 " + remotePath}
	}
	var last error
	for _, cmd := range cmds {
		code, line, err := command(h.text, cmd)
		if err != nil {
			last = err
			continue
		}
		if code >= 200 && code < 300 {
			if sum := extractHex(line); sum != "" {
				return sum, true, nil
			}
		}
		last = fmt.Errorf("ftp: %s returned %d %s", cmd, code, line)
	}
	if last != nil {
		return "", false, last
	}
	return "", false, nil
}

func (h *hashClient) Close() error {
	if h == nil || h.text == nil {
		return nil
	}
	_, _ = h.text.Cmd("QUIT")
	err := h.text.Close()
	h.text = nil
	return err
}

func chooseAlgo(have map[string]bool, want string) string {
	want = strings.ToLower(strings.TrimSpace(want))
	if want != "" && want != config.AlgoAuto {
		if have[want] {
			return want
		}
		return ""
	}
	for _, algo := range []string{"sha256", "sha1", "md5"} {
		if have[algo] {
			return algo
		}
	}
	return ""
}

func login(text *textproto.Conn, user, password string) error {
	code, _, err := command(text, "USER "+user)
	if err != nil {
		return err
	}
	switch code {
	case 230:
		return nil
	case 331:
		code, msg, err := command(text, "PASS "+password)
		if err != nil {
			return err
		}
		if code != 230 {
			return fmt.Errorf("ftp: login rejected: %d %s", code, msg)
		}
		return nil
	default:
		return fmt.Errorf("ftp: login rejected with %d", code)
	}
}

func feat(text *textproto.Conn) (map[string]string, error) {
	code, line, err := command(text, "FEAT")
	if err != nil {
		return nil, err
	}
	features := map[string]string{}
	if code != 211 {
		return features, nil
	}
	for _, row := range strings.Split(line, "\n") {
		row = strings.TrimRight(row, "\r")
		if !strings.HasPrefix(row, " ") {
			continue
		}
		row = strings.TrimSpace(row)
		name, desc, _ := strings.Cut(row, " ")
		features[strings.ToUpper(name)] = desc
	}
	return features, nil
}

func hasKey(features map[string]string, key string) bool {
	_, ok := features[key]
	return ok
}

func cmdOK(text *textproto.Conn, expect int, cmd string) error {
	code, _, err := command(text, cmd)
	if err != nil {
		return err
	}
	if code != expect {
		return fmt.Errorf("ftp: %s returned %d, want %d", cmd, code, expect)
	}
	return nil
}

func command(text *textproto.Conn, cmd string) (int, string, error) {
	if strings.ContainsAny(cmd, "\r\n") {
		return 0, "", errors.New("ftp: command contains a newline")
	}
	if _, err := text.Cmd("%s", cmd); err != nil {
		return 0, "", err
	}
	code, msg, err := text.ReadResponse(0)
	return code, msg, err
}

func extractHex(text string) string {
	for _, field := range strings.Fields(text) {
		field = strings.Trim(field, "*")
		if len(field) != 32 && len(field) != 40 && len(field) != 64 {
			continue
		}
		ok := true
		for _, r := range field {
			switch {
			case r >= '0' && r <= '9', r >= 'a' && r <= 'f', r >= 'A' && r <= 'F':
			default:
				ok = false
			}
		}
		if ok {
			return strings.ToLower(field)
		}
	}
	return ""
}

// bufConn lets textproto.Conn own a TLS connection without double-closing
// confusion in tests. It is a plain passthrough.
type bufConn struct{ net.Conn }

func (c bufConn) Close() error { return c.Conn.Close() }
