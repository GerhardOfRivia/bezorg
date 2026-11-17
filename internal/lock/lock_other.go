//go:build !unix

package lock

import (
	"fmt"
	"os"
)

// Exclusive is unavailable on this platform.
func Exclusive(path string) (*os.File, error) {
	return nil, fmt.Errorf("lock %s: file locking is not implemented on this platform", path)
}
