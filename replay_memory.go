package aigate

import (
	"context"
	"sync"
)

const replayBodyBudget = 512 << 20

// Shared by all senders. Waiting for memory is interruptible by run stop.
type replayByteBudget struct {
	mu      sync.Mutex
	used    int64
	changed chan struct{}
}

func newReplayByteBudget() *replayByteBudget {
	return &replayByteBudget{changed: make(chan struct{})}
}

func (b *replayByteBudget) acquire(ctx context.Context, n int64) bool {
	if n > replayBodyBudget {
		return false
	}
	for ctx.Err() == nil {
		b.mu.Lock()
		if b.used+n <= replayBodyBudget {
			b.used += n
			b.mu.Unlock()
			return true
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
	return false
}

func (b *replayByteBudget) release(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used -= n
	close(b.changed)
	b.changed = make(chan struct{})
}
