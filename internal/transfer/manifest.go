package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ManifestVersion is the version written into every completion manifest.
const ManifestVersion = 1

// Manifest is the completion marker for one download. Bezorg writes it only
// after every file in the job has its final name. The file name is the job's
// local path plus the done suffix, for example /data/incoming.bezorg-done.json
// when local is the directory /data/incoming, or
// /data/incoming/tile.tif.bezorg-done.json when local is that file.
// Appearance of this file means the delivery is complete.
//
// external_id and job_id are present even when empty. remote is the remote
// path, or a URL with any userinfo removed. completed_at is RFC3339 UTC.
// files[].path is relative to the directory that contains the manifest and
// uses forward slashes. files[].sha256 is lowercase hex of the named file.
type Manifest struct {
	Version     int            `json:"version"`
	JobID       string         `json:"job_id"`
	ExternalID  string         `json:"external_id"`
	Remote      string         `json:"remote"`
	CompletedAt string         `json:"completed_at"`
	Files       []ManifestFile `json:"files"`
}

// ManifestFile is one object in a completion manifest.
type ManifestFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// ManifestPath is the final name of the completion marker.
func ManifestPath(local, suffix string) string {
	return local + suffix
}

func (e *engine) publishManifest(now time.Time) (string, error) {
	if e.cfg.DryRun || e.cfg.Local == "" {
		return "", nil
	}
	dest := ManifestPath(e.cfg.Local, e.cfg.DoneSuffix)
	dir := filepath.Dir(dest)
	e.mu.Lock()
	saved := append([]string(nil), e.saved...)
	e.mu.Unlock()
	files := make([]ManifestFile, 0, len(saved))
	for _, final := range saved {
		sum, size, err := fileSHA256(final)
		if err != nil {
			return "", err
		}
		rel, err := filepath.Rel(dir, final)
		if err != nil {
			return "", err
		}
		rel = filepath.ToSlash(rel)
		if rel == ".." || strings.HasPrefix(rel, "../") || strings.HasPrefix(rel, "/") {
			return "", os.ErrInvalid
		}
		files = append(files, ManifestFile{Path: rel, Size: size, SHA256: sum})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	if now.IsZero() {
		now = time.Now()
	}
	body, err := json.MarshalIndent(Manifest{
		Version:     ManifestVersion,
		JobID:       e.jobID,
		ExternalID:  e.externalID,
		Remote:      remoteSource(e.cfg.Remote),
		CompletedAt: now.UTC().Format(time.RFC3339),
		Files:       files,
	}, "", "  ")
	if err != nil {
		return "", err
	}
	body = append(body, '\n')
	partial := dest + e.cfg.TempSuffix
	if err := writeAtomicFile(partial, body, 0o644); err != nil {
		return "", err
	}
	if e.beforeManifestRename != nil {
		if err := e.beforeManifestRename(); err != nil {
			_ = os.Remove(partial)
			return "", err
		}
	}
	if err := os.Rename(partial, dest); err != nil {
		_ = os.Remove(partial)
		return "", err
	}
	syncDir(dir)
	return dest, nil
}

func fileSHA256(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}

// remoteSource drops URL userinfo so a manifest never carries credentials.
func remoteSource(remote string) string {
	remote = strings.TrimSpace(remote)
	parsed, err := url.Parse(remote)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return remote
	}
	parsed.User = nil
	return parsed.String()
}

func writeAtomicFile(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}
