package transfer

import (
	"context"
	"time"
)

// Gate caps how many sessions may be open. Penalize pulls one slot out of
// circulation for a cooldown after the server reports a connection limit.
type Gate struct {
	tokens chan struct{}
	max    int

	mu        chan struct{}
	penalties int
}

func newGate(n int) *Gate {
	if n < 1 {
		n = 1
	}
	g := &Gate{
		tokens: make(chan struct{}, n),
		max:    n,
		mu:     make(chan struct{}, 1),
	}
	g.mu <- struct{}{}
	for i := 0; i < n; i++ {
		g.tokens <- struct{}{}
	}
	return g
}

func (g *Gate) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.tokens:
		return nil
	}
}

func (g *Gate) Release() {
	g.tokens <- struct{}{}
}

// Penalize holds one permit for cooldown, down to a single remaining slot.
func (g *Gate) Penalize(ctx context.Context, cooldown time.Duration) {
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	select {
	case <-g.mu:
	case <-ctx.Done():
		return
	}
	if g.penalties >= g.max-1 {
		g.mu <- struct{}{}
		return
	}
	g.penalties++
	g.mu <- struct{}{}

	go func() {
		defer func() {
			<-g.mu
			g.penalties--
			g.mu <- struct{}{}
		}()
		select {
		case <-ctx.Done():
			return
		case <-g.tokens:
		}
		timer := time.NewTimer(cooldown)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		g.tokens <- struct{}{}
	}()
}
