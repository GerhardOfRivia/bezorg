package transfer

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// idSidecarSuffix is the sibling written next to a finished download when the
// job carries an external id. Onderzeeer should exclude this suffix and
// *.bezorg-partial, then read the sidecar from a file-drop hook to store the
// same external_id. The sidecar is renamed into place before the data file,
// so a watcher that sees the final name can already read the id.
const idSidecarSuffix = ".bezorg-id"

// idSidecar is the correlation file onderzeeer (or another agent) reads.
type idSidecar struct {
	ExternalID string `json:"external_id"`
	JobID      string `json:"job_id,omitempty"`
	Remote     string `json:"remote"`
	Local      string `json:"local"`
}

// writeIDSidecar atomically publishes <final>.bezorg-id. An empty external id
// writes nothing, which leaves downloads that do not need correlation unchanged.
// The temporary name uses the download's temp suffix so an exclude of
// *.bezorg-partial also ignores a sidecar that has not been renamed yet.
func writeIDSidecar(final, tempSuffix, externalID, jobID, remote string) error {
	if externalID == "" {
		return nil
	}
	body, err := json.MarshalIndent(idSidecar{
		ExternalID: externalID,
		JobID:      jobID,
		Remote:     remote,
		Local:      final,
	}, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	dest := final + idSidecarSuffix
	partial := dest + tempSuffix
	if err := writeAtomicFile(partial, body, 0o644); err != nil {
		return err
	}
	if err := os.Rename(partial, dest); err != nil {
		_ = os.Remove(partial)
		return err
	}
	syncDir(filepath.Dir(dest))
	return nil
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
