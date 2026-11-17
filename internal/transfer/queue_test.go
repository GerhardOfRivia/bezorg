package transfer

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestQueuePrefersSmallLaneWhileLargeIsCapped(t *testing.T) {
	var files []remoteFile
	for i := 0; i < 6; i++ {
		files = append(files, remoteFile{rel: "s", size: 1})
	}
	files = append(files, remoteFile{rel: "L1", size: 100, large: true}, remoteFile{rel: "L2", size: 100, large: true})
	q := newWorkQueue(files, 4, 1)

	var mu sync.Mutex
	var got []remoteFile
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			file, ok := q.Pop(ctx)
			if !ok {
				return
			}
			mu.Lock()
			got = append(got, file)
			mu.Unlock()
			if file.large {
				time.Sleep(100 * time.Millisecond)
			}
			q.Done(file.large)
		}()
	}
	wg.Wait()
	large := 0
	for _, file := range got {
		if file.large {
			large++
		}
	}
	if large > 1 {
		t.Fatalf("started %d large files in the first wave; cap is 1", large)
	}
	if len(got) != 4 {
		t.Fatalf("started %d files, want 4", len(got))
	}
}
