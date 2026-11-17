// Package paths resolves the daemon socket and state directory.
package paths

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// StateDir returns the directory that holds job records.
// An explicit path wins, then BEZORG_STATE_DIR, then the XDG state home.
func StateDir(explicit string) string {
	if value := strings.TrimSpace(explicit); value != "" {
		return value
	}
	if value := strings.TrimSpace(os.Getenv("BEZORG_STATE_DIR")); value != "" {
		return value
	}
	if value := strings.TrimSpace(os.Getenv("XDG_STATE_HOME")); value != "" {
		return filepath.Join(value, "bezorg")
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".local", "state", "bezorg")
	}
	return filepath.Join(os.TempDir(), fmt.Sprintf("bezorg-%d", os.Getuid()))
}

// Socket returns the daemon's Unix socket path.
// An explicit path wins, then BEZORG_SOCKET, then the XDG runtime directory.
func Socket(explicit string) string {
	if value := strings.TrimSpace(explicit); value != "" {
		return value
	}
	if value := strings.TrimSpace(os.Getenv("BEZORG_SOCKET")); value != "" {
		return value
	}
	if value := strings.TrimSpace(os.Getenv("XDG_RUNTIME_DIR")); value != "" {
		return filepath.Join(value, "bezorg", "bezorg.sock")
	}
	return filepath.Join(os.TempDir(), fmt.Sprintf("bezorg-%d", os.Getuid()), "bezorg.sock")
}
