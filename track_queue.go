package raindrop

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	// defaultTrackBatchSize caps the items in one events/track or
	// signals/track request.
	defaultTrackBatchSize = 100
	// defaultTrackQueueSize bounds the queue: past it, new items are dropped
	// with a warning rather than letting memory grow without limit.
	defaultTrackQueueSize = 10_000
)

const (
	routeEventsTrack   = "events/track"
	routeSignalsTrack  = "signals/track"
	routeUsersIdentify = "users/identify"
)

type queuedItem struct {
	route   string
	payload any
}

// trackQueue holds complete events, signals and identify calls. Enqueue is
// the hot path (CONTRACT 1.1): it only appends under a mutex. A background
// goroutine sends batches, and Flush/Stop drain whatever is queued.
type trackQueue struct {
	client *Client

	mu       sync.Mutex
	items    []queuedItem
	maxQueue int
	maxBatch int

	// sendSem admits one drain at a time, so an explicit Flush waits for a
	// background send already in flight instead of returning while its
	// payloads are neither queued nor delivered (CONTRACT 1.2).
	sendSem chan struct{}

	ticker   *time.Ticker
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
	kickCh   chan struct{}

	runCtx    context.Context
	runCancel context.CancelFunc
}

func newTrackQueue(client *Client, flushEvery time.Duration, maxBatch, maxQueue int) *trackQueue {
	runCtx, runCancel := context.WithCancel(context.Background())
	q := &trackQueue{
		client:    client,
		maxQueue:  maxQueue,
		maxBatch:  maxBatch,
		sendSem:   make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		kickCh:    make(chan struct{}, 1),
		runCtx:    runCtx,
		runCancel: runCancel,
	}
	if client != nil && client.enabled && flushEvery > 0 {
		q.ticker = time.NewTicker(flushEvery)
		go q.run()
	}
	return q
}

func (q *trackQueue) run() {
	defer close(q.doneCh)
	for {
		select {
		case <-q.stopCh:
			q.ticker.Stop()
			return
		case <-q.ticker.C:
			_ = q.drain(q.runCtx, true)
		case <-q.kickCh:
			_ = q.drain(q.runCtx, true)
		}
	}
}

// Enqueue queues one item without any network I/O on the caller.
func (q *trackQueue) Enqueue(route string, payload any) {
	q.mu.Lock()
	if len(q.items) >= q.maxQueue {
		q.mu.Unlock()
		q.client.warn("track.queue_full",
			"raindrop: telemetry queue is full; dropping the item", "route", route, "queue_size", q.maxQueue)
		return
	}
	q.items = append(q.items, queuedItem{route: route, payload: payload})
	full := len(q.items) >= q.maxBatch
	q.mu.Unlock()

	if full && q.ticker != nil {
		select {
		case q.kickCh <- struct{}{}:
		default:
		}
	}
}

// Flush sends everything queued before it, after any send in flight.
func (q *trackQueue) Flush(ctx context.Context) error {
	return q.drain(ctx, false)
}

// Stop halts the background sender and drains the queue under ctx. A
// background send in flight is cancelled; its items are put back and sent
// here under the caller's deadline.
func (q *trackQueue) Stop(ctx context.Context) error {
	q.stopOnce.Do(func() {
		close(q.stopCh)
		q.runCancel()
	})
	if q.ticker != nil {
		<-q.doneCh
	}
	return q.drain(ctx, false)
}

func (q *trackQueue) drain(ctx context.Context, background bool) error {
	select {
	case q.sendSem <- struct{}{}:
	case <-ctx.Done():
		q.client.warn("track.flush_deadline",
			"raindrop: flush deadline passed while a send was in flight")
		return ctx.Err()
	}
	defer func() { <-q.sendSem }()

	var firstErr error
	for {
		q.mu.Lock()
		pending := q.items
		q.items = nil
		q.mu.Unlock()
		if len(pending) == 0 {
			return firstErr
		}
		for len(pending) > 0 {
			batch, route, payload := q.nextBatch(pending)
			err := q.client.transport.postJSON(ctx, route, payload)
			if err == nil {
				pending = pending[len(batch):]
				continue
			}
			var statusErr *httpStatusError
			permanent := errors.As(err, &statusErr) && !isRetryableStatus(statusErr.StatusCode)
			if firstErr == nil && !permanent {
				// A permanent rejection is a deliberate, warned drop, not a
				// delivery failure for the caller to handle.
				firstErr = err
			}
			if background && ctx.Err() != nil {
				// Stop cancelled this send: put everything unsent back for
				// Stop's own bounded drain.
				q.restore(pending)
				return firstErr
			}
			if !permanent {
				// Permanent rejections already warned in the transport.
				q.client.warn(route+".dropped",
					"raindrop: could not deliver telemetry; dropping it", "route", route, "items", len(batch), "error", err)
			}
			pending = pending[len(batch):]
		}
	}
}

// nextBatch takes the leading run of items for one route: events and
// signals go as one array per request (CONTRACT 7.2), identify one at a time.
func (q *trackQueue) nextBatch(pending []queuedItem) ([]queuedItem, string, any) {
	route := pending[0].route
	if route == routeUsersIdentify {
		return pending[:1], route, pending[0].payload
	}
	n := 0
	for n < len(pending) && n < q.maxBatch && pending[n].route == route {
		n++
	}
	body := make([]any, n)
	for i := 0; i < n; i++ {
		body[i] = pending[i].payload
	}
	return pending[:n], route, body
}

func (q *trackQueue) restore(items []queuedItem) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(append([]queuedItem{}, items...), q.items...)
	if len(q.items) > q.maxQueue {
		q.items = q.items[:q.maxQueue]
	}
}
