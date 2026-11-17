package transfer

import (
	"errors"
	"path"
	"strings"
)

var (
	errEmptyRel  = errors.New("empty relative path")
	errEscapeRel = errors.New("remote path escapes destination")
)

const imageryFloor = 8 << 20

// Action is the local-file decision for one remote object.
type Action int

const (
	// ActionFetch downloads from Offset. Offset zero replaces any partial.
	ActionFetch Action = iota
	// ActionSkip leaves an existing final file in place.
	ActionSkip
	// ActionFinish verifies a partial that already has every remote byte,
	// then renames it into place.
	ActionFinish
)

// LocalPlan is how a worker should treat files already on disk.
type LocalPlan struct {
	Action Action
	Offset int64
}

// PlanLocal decides whether to skip, resume, or rewrite a file.
// remoteSize below zero means the server did not report a length.
// A listed size of zero is an empty object.
func PlanLocal(finalExists bool, finalSize int64, partialExists bool, partialSize, remoteSize int64) LocalPlan {
	if finalExists && sizeMatches(finalSize, remoteSize) {
		return LocalPlan{Action: ActionSkip}
	}
	if partialExists && remoteSize >= 0 && partialSize == remoteSize {
		return LocalPlan{Action: ActionFinish}
	}
	if partialExists && remoteSize >= 0 && partialSize < remoteSize {
		return LocalPlan{Action: ActionFetch, Offset: partialSize}
	}
	if partialExists && remoteSize < 0 && partialSize > 0 {
		return LocalPlan{Action: ActionFetch, Offset: partialSize}
	}
	return LocalPlan{Action: ActionFetch, Offset: 0}
}

func sizeMatches(local, remote int64) bool {
	if remote > 0 {
		return local == remote
	}
	if remote == 0 {
		return local == 0
	}
	// Unknown remote length: keep a non-empty local file. Re-runs of an
	// immutable delivery must not download it again.
	return local > 0
}

// IsLarge reports whether a file should use the bounded large-file lane.
// Imagery extensions join that lane once they pass a modest floor, so a
// multi-gigabyte GeoTIFF cannot occupy every connection, and a tiny preview
// TIFF still shares the small-file workers.
func IsLarge(name string, size, minBytes int64) bool {
	if minBytes < 1 {
		minBytes = 64 << 20
	}
	if size >= minBytes {
		return true
	}
	switch strings.ToLower(path.Ext(name)) {
	case ".tif", ".tiff", ".ntf":
		if size < 0 {
			return true
		}
		floor := int64(imageryFloor)
		if minBytes < floor {
			floor = minBytes
		}
		return size >= floor
	default:
		return false
	}
}

// CleanRel rejects absolute paths and any relative path that escapes the
// destination root.
func CleanRel(rel string) (string, error) {
	rel = strings.ReplaceAll(rel, "\\", "/")
	if strings.HasPrefix(rel, "/") {
		return "", errEscapeRel
	}
	var parts []string
	for _, part := range strings.Split(rel, "/") {
		switch part {
		case "", ".":
			continue
		case "..":
			return "", errEscapeRel
		default:
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 {
		return "", errEmptyRel
	}
	return strings.Join(parts, "/"), nil
}
