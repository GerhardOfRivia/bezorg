package job

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/GerhardOfRivia/bezorg/internal/config"
)

func TestExternalIDIsStoredIndexedAndOmittedWhenEmpty(t *testing.T) {
	store, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Host = "ftp.example.com"
	cfg.Remote = "/drop/a"
	cfg.Local = t.TempDir()
	cfg.Password = "secret"
	cfg.ExternalID = "  batch-42  "

	first, err := store.Create(cfg)
	if err != nil || first.ExternalID != "batch-42" || first.Config.ExternalID != "" || first.Config.Password != "secret" {
		t.Fatalf("created = %+v err=%v", first, err)
	}
	cfg.ExternalID = "batch-42"
	cfg.Remote = "/drop/b"
	second, err := store.Create(cfg)
	if !errors.Is(err, ErrExternalIDInUse) || second.ID != first.ID || second.ExternalID != "batch-42" {
		t.Fatalf("duplicate id = %+v %v", second, err)
	}
	cfg.ExternalID = ""
	plain, err := store.Create(cfg)
	if err != nil || plain.ExternalID != "" {
		t.Fatalf("plain = %+v %v", plain, err)
	}

	matched, err := store.ListByExternalID(" batch-42 ")
	if err != nil || len(matched) != 1 {
		t.Fatalf("matched = %+v %v", matched, err)
	}
	if matched[0].ID != first.ID {
		t.Fatalf("match = %+v", matched[0])
	}
	none, err := store.ListByExternalID("missing")
	if err != nil || len(none) != 0 {
		t.Fatalf("none = %+v %v", none, err)
	}
	if _, err := store.ListByExternalID(strings.Repeat("x", config.MaxExternalIDBytes+1)); err == nil {
		t.Fatal("oversized filter accepted")
	}

	reopened, err := Open(store.dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.Get(first.ID)
	if err != nil || loaded.ExternalID != "batch-42" {
		t.Fatalf("reloaded = %+v %v", loaded, err)
	}
	again, err := reopened.ListByExternalID("batch-42")
	if err != nil || len(again) != 1 || again[0].ID != first.ID {
		t.Fatalf("reindexed = %+v %v", again, err)
	}
	next := cfg
	next.ExternalID = ""
	next.Remote = "/drop/c"
	withMeta, err := store.CreateWith(next, 4, json.RawMessage(`{"source":"bezorg"}`))
	if err != nil || withMeta.Priority != 4 || !strings.Contains(string(withMeta.Metadata), "bezorg") {
		t.Fatalf("metadata = %+v %v", withMeta, err)
	}
	if _, err := store.CreateWith(next, 4, json.RawMessage(`[]`)); err == nil {
		t.Fatal("metadata array accepted")
	}
	public := Public(loaded)
	if public.ExternalID != "batch-42" || public.Config.Password == "secret" {
		t.Fatalf("public copy = %+v", public.Config)
	}
	if public.CreatedAt.IsZero() || public.UpdatedAt.Before(time.Time{}) {
		t.Fatal("timestamps missing")
	}
}
