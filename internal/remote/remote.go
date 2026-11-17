// Package remote is the protocol-neutral transfer surface used by the engine.
// FTP and SFTP clients both implement Session.
package remote

import (
	"context"
	"io"
)

// File is one remote object to mirror under the local destination.
type File struct {
	// Rel is a cleaned slash-separated path relative to the local root.
	Rel string `json:"rel"`
	// Remote is the path passed to RETR / SFTP open.
	Remote string `json:"remote"`
	// Size is the remote length in bytes. Values below zero mean unknown.
	Size int64 `json:"size"`
	// Local, when set, is an absolute or cwd-relative file path that replaces
	// the usual local-root join. Used for a single remote file saved under an
	// explicit filename.
	Local string `json:"local,omitempty"`
}

// Listing is the result of walking a remote path.
type Listing struct {
	Files      []File
	RootIsFile bool
}

// Session is one protocol connection. Implementations are not safe for
// concurrent use. The engine gives each worker its own session.
type Session interface {
	List(ctx context.Context, remote string) (Listing, error)
	// OpenAt starts a streaming read at offset. Offset zero reads from the
	// start. The caller must Close the result, including after a copy error.
	OpenAt(ctx context.Context, remotePath string, offset int64) (io.ReadCloser, error)
	// ReadSmall reads a short remote object, such as a checksum sidecar.
	// It returns an error if the object is larger than limit.
	ReadSmall(ctx context.Context, remotePath string, limit int64) ([]byte, error)
	Close() error
}

// Checksummer fetches a server-side digest. ok is false when the server
// cannot produce one; that is not an error.
type Checksummer interface {
	Sum(ctx context.Context, remotePath, algo string) (sum string, ok bool, err error)
	Close() error
}
