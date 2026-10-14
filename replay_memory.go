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

// Timed dispatch never queues on memory. Reserve before creating its sender.
func (b *replayByteBudget) tryAcquire(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 || n > replayBodyBudget-b.used {
		return false
	}
	b.used += n
	return true
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
