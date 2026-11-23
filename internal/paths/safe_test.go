package paths

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSafeLocalRejectsRelativeRootAndStateDir(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(root, "incoming")
	if err := os.Mkdir(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := SafeLocal(filepath.Join(dest, "file.bin"), state)
	if err != nil || got != filepath.Join(dest, "file.bin") {
		t.Fatalf("got %s %v", got, err)
	}
	if _, err := SafeLocal("relative.bin", state); err == nil {
		t.Fatal("relative path accepted")
	}
	if _, err := SafeLocal("/", state); err == nil {
		t.Fatal("root accepted")
	}
	if _, err := SafeLocal(filepath.Join(state, "jobs", "secret"), state); err == nil {
		t.Fatal("state directory accepted")
	}
	if _, err := SafeLocal("/no-such-drive\x00/file", state); err == nil {
		t.Fatal("NUL accepted")
	}

	outside := filepath.Join(root, "outside")
	if err := os.Mkdir(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dest, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	resolved, err := SafeLocal(filepath.Join(link, "tile.tif"), state)
	if err != nil || resolved != filepath.Join(outside, "tile.tif") {
		t.Fatalf("symlink dest = %s %v", resolved, err)
	}
}
