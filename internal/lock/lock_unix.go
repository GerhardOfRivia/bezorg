//go:build unix

package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// Exclusive creates path and takes a non-blocking exclusive flock.
// The returned file must stay open for as long as the lock is held.
func Exclusive(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return file, nil
}
