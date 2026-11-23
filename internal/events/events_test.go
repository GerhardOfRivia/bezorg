package events

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLogIsMonotonicDurableAndDoesNotReuseSeq(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	log.keep = 3
	first, err := log.Append(sample("download.queued", "job-a"))
	if err != nil || first.Seq != 1 || first.ExternalID != "batch-42" {
		t.Fatalf("first = %+v %v", first, err)
	}
	for i := 0; i < 4; i++ {
		if _, err := log.Append(sample("download.started", "job-a")); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.pruneLocked(); err != nil {
		t.Fatal(err)
	}
	kept, err := log.List(0, 100)
	if err != nil || len(kept) != 3 || kept[0].Seq != 3 || kept[2].Seq != 5 {
		t.Fatalf("kept = %+v %v", kept, err)
	}
	next, err := log.Append(sample("download.succeeded", "job-a"))
	if err != nil || next.Seq != 6 {
		t.Fatalf("next = %+v %v", next, err)
	}

	torn := filepath.Join(dir, "log.jsonl")
	file, err := os.OpenFile(torn, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("{\"seq\":99,\"type\":\"download.failed\""); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	reopened, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	reopened.keep = 10
	again, err := reopened.List(0, 100)
	if err != nil || len(again) == 0 || again[len(again)-1].Seq != 6 {
		t.Fatalf("reopened = %+v %v", again, err)
	}
	resumed, err := reopened.Append(sample("download.retried", "job-b"))
	if err != nil || resumed.Seq != 7 {
		t.Fatalf("resumed seq = %+v %v", resumed, err)
	}
	page, err := reopened.List(6, 1)
	if err != nil || len(page) != 1 || page[0].Seq != 7 || page[0].Type != DownloadRetried {
		t.Fatalf("page = %+v %v", page, err)
	}
	if _, err := reopened.List(-1, 10); err == nil {
		t.Fatal("negative cursor accepted")
	}
}

func sample(kind, id string) Event {
	started := time.Unix(10, 0).UTC()
	return Event{
		Type: kind, JobID: id, ExternalID: "batch-42", Remote: "/drop/a.txt", Local: "/data/a.txt",
		Status: "queued", Attempts: 1, Bytes: 5, JobCreatedAt: time.Unix(1, 0).UTC(), UpdatedAt: time.Unix(2, 0).UTC(),
		StartedAt: &started,
	}
}

func TestKnownTypesStayStable(t *testing.T) {
	names := []string{DownloadQueued, DownloadStarted, DownloadSucceeded, DownloadFailed, DownloadRetried, DownloadInterrupted}
	joined := strings.Join(names, " ")
	for _, name := range []string{"download.queued", "download.started", "download.succeeded", "download.failed", "download.retried", "download.interrupted"} {
		if !strings.Contains(joined, name) {
			t.Fatalf("missing %s", name)
		}
	}
}
