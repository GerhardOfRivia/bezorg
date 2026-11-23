package paths

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// SafeLocal checks a download destination. The path must be absolute, must
// not be the filesystem root, and must not resolve inside the daemon state
// directory. Symlinks in the existing prefix are resolved and the remainder
// of the path is joined underneath that directory, so a ".." component cannot
// escape it. The returned path is the one the download should write.
func SafeLocal(path, stateDir string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" || strings.ContainsRune(path, 0) || !filepath.IsAbs(path) {
		return "", errors.New("destination must be an absolute path")
	}
	cleaned := filepath.Clean(path)
	if cleaned == string(filepath.Separator) {
		return "", errors.New("destination must not be the filesystem root")
	}
	existing := cleaned
	var suffix []string
	for {
		info, err := os.Lstat(existing)
		if err == nil {
			if len(suffix) == 0 && !info.IsDir() && !info.Mode().IsRegular() {
				return "", errors.New("destination must be a file or directory")
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			return "", errors.New("destination is not inside an existing directory")
		}
		suffix = append([]string{filepath.Base(existing)}, suffix...)
		existing = parent
	}
	resolved, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if len(suffix) > 0 && !info.IsDir() {
		return "", errors.New("destination parent is not a directory")
	}
	final := resolved
	for _, part := range suffix {
		if part == "" || part == "." || part == ".." {
			return "", errors.New("destination must not escape its directory")
		}
		final = filepath.Join(final, part)
	}
	relative, err := filepath.Rel(resolved, final)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("destination escapes the resolved directory")
	}
	if strings.TrimSpace(stateDir) != "" {
		state := stateDir
		if resolvedState, stateErr := filepath.EvalSymlinks(stateDir); stateErr == nil {
			state = resolvedState
		}
		inside, err := filepath.Rel(state, final)
		if err == nil && inside != ".." && !strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
			return "", errors.New("destination must not be inside the bezorg state directory")
		}
	}
	return final, nil
}
