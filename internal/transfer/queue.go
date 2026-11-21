package transfer

import (
	"context"
	"sync"
)

type queuedFile struct {
	file  remoteFile
	large bool
}

// remoteFile is the engine's view of one listing entry.
type remoteFile struct {
	rel    string
	remote string
	local  string
	size   int64
	large  bool
}

// workQueue hands workers a mix of small and large files.
// At most maxLarge large files are in flight, and the remaining workers
// stay on the small-file lane so a multi-gigabyte TIFF cannot stall the drop.
type workQueue struct {
	mu          sync.Mutex
	cond        *sync.Cond
	small       []remoteFile
	large       []remoteFile
	smallActive int
	largeActive int
	maxLarge    int
	workers     int
	closed      bool
}

func newWorkQueue(files []remoteFile, workers, maxLarge int) *workQueue {
	q := &workQueue{workers: workers, maxLarge: maxLarge}
	if q.maxLarge < 1 {
		q.maxLarge = 1
	}
	if q.maxLarge > workers {
		q.maxLarge = workers
	}
	q.cond = sync.NewCond(&q.mu)
	for _, file := range files {
		if file.large {
			q.large = append(q.large, file)
		} else {
			q.small = append(q.small, file)
		}
	}
	return q
}

func (q *workQueue) Pop(ctx context.Context) (remoteFile, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return remoteFile{}, false
		}
		file, ok := q.pickLocked()
		if ok {
			return file, true
		}
		if len(q.small) == 0 && len(q.large) == 0 {
			return remoteFile{}, false
		}
		q.cond.Wait()
	}
}

func (q *workQueue) pickLocked() (remoteFile, bool) {
	smallBudget := q.workers - q.maxLarge
	if smallBudget < 0 {
		smallBudget = 0
	}
	canSmall := len(q.small) > 0
	canLarge := len(q.large) > 0 && q.largeActive < q.maxLarge
	switch {
	case canLarge && (!canSmall || q.smallActive >= smallBudget):
		return q.popLarge(), true
	case canSmall:
		return q.popSmall(), true
	case canLarge:
		return q.popLarge(), true
	default:
		return remoteFile{}, false
	}
}

func (q *workQueue) popSmall() remoteFile {
	file := q.small[0]
	q.small = q.small[1:]
	q.smallActive++
	file.large = false
	return file
}

func (q *workQueue) popLarge() remoteFile {
	file := q.large[0]
	q.large = q.large[1:]
	q.largeActive++
	file.large = true
	return file
}

func (q *workQueue) Done(large bool) {
	q.mu.Lock()
	if large {
		if q.largeActive > 0 {
			q.largeActive--
		}
	} else if q.smallActive > 0 {
		q.smallActive--
	}
	q.cond.Broadcast()
	q.mu.Unlock()
}

func (q *workQueue) wake() {
	q.mu.Lock()
	q.cond.Broadcast()
	q.mu.Unlock()
}

func (q *workQueue) depth() (small, large int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.small), len(q.large)
}

// drain drops files still waiting and reports how many were left in each lane.
func (q *workQueue) drain() (small, large int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	small, large = len(q.small), len(q.large)
	q.small = nil
	q.large = nil
	q.closed = true
	q.cond.Broadcast()
	return small, large
}
