package queue

import (
	"container/list"
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

func New(initialConcurrency int, concurrencyPerReplica uint32) *Queue {
	return &Queue{
		concurrency:           initialConcurrency,
		concurrencyPerReplica: concurrencyPerReplica,
		list:                  list.New(),
		enqueued:              make(chan struct{}),
		completed:             make(chan struct{}),
	}
}

// Queue is a thread-safe FIFO queue that will limit the number of concurrent
// requests that can be in progress at any given time.
type Queue struct {
	concurrencyMtx sync.RWMutex
	// concurrency is the max number of in progress items.
	concurrency           int
	concurrencyPerReplica uint32

	list    *list.List
	listMtx sync.Mutex

	// enqueued signals when a new item is enqueued.
	enqueued chan struct{}

	// completed signals when a item that has been dequeued has completed.
	completed chan struct{}

	// totalCount is the number of requests that have been enqueued
	// but not yet completed.
	totalCount atomic.Int64

	// inProgressCount is the number of requests that have been dequeued
	// but not yet completed.
	inProgressCount atomic.Int64
}

type item struct {
	id string
	e  *list.Element

	// dequeued is a channel that signals this item was removed from the queue.
	// It is used to block EnqueueAndWait().
	dequeued chan struct{}

	inProgress bool

	closed bool
}

func (q *Queue) dequeue(itm *item, inProgress bool) {
	if inProgress {
		q.inProgressCount.Add(1)
	}
	q.listMtx.Lock()
	itm.inProgress = inProgress
	q.list.Remove(itm.e)
	if !itm.closed {
		close(itm.dequeued)
		itm.closed = true
	}
	q.listMtx.Unlock()
}

// EnqueueAndWait adds an item to the queue and waits for it to be dequeued.
// It returns a function that should be called after all work has completed.
// The id parameter is only used for tracking/debugging purposes.
func (q *Queue) EnqueueAndWait(ctx context.Context, id string) func() {
	q.totalCount.Add(1)
	itm := &item{
		id:       id,
		dequeued: make(chan struct{}),
	}
	q.listMtx.Lock()
	itm.e = q.list.PushBack(itm)
	q.listMtx.Unlock()

	// Send a notification on the enqueued channel only if the central loop
	// is waiting for an item to be enqueued (only happens when queue is empty).
	select {
	case q.enqueued <- struct{}{}:
	default:
	}

	// Wait to be dequeued.
	select {
	case <-itm.dequeued:
	case <-ctx.Done():
		q.dequeue(itm, false)
	}

	return q.completeFunc(itm)
}

func (q *Queue) completeFunc(itm *item) func() {
	return func() {
		log.Println("Running completeFunc: ", itm.id)
		q.totalCount.Add(-1)

		log.Println("Locking queue.list: ", itm.id)
		q.listMtx.Lock()
		if !itm.closed {
			log.Println("Closing item.dequeued: ", itm.id)
			close(itm.dequeued)
			itm.closed = true
		}

		inProgress := itm.inProgress
		log.Printf("Item %v inProgress: %v\n", itm.id, inProgress)
		q.listMtx.Unlock()

		if inProgress {
			q.inProgressCount.Add(-1)

			// Make sure we only send a message on the completed channel if the
			// item was counted as inProgress.
			select {
			case q.completed <- struct{}{}:
				log.Println("Sent completed message: ", itm.id)
			default:
				log.Println("Did not send completed message: ", itm.id)
			}
		}

		log.Println("Finished completeFunc: ", itm.id)
	}
}

func (q *Queue) Start() {
	for {
		if q.inProgressCount.Load() >= int64(q.GetConcurrency()) {
			log.Println("Waiting for requests to complete")
			<-q.completed
			continue
		}

		q.listMtx.Lock()
		e := q.list.Front()
		q.listMtx.Unlock()

		if e == nil {
			// Queue is empty, wait until something is enqueued.
			<-q.enqueued
			continue
		}

		itm := e.Value.(*item)
		q.dequeue(itm, true)
		log.Println("Dequeued: ", itm.id)

		time.Sleep(time.Second / 100)
	}
}

func (q *Queue) GetConcurrency() int {
	q.concurrencyMtx.RLock()
	defer q.concurrencyMtx.RUnlock()
	return q.concurrency
}

func (q *Queue) setConcurrency(n int) {
	q.concurrencyMtx.Lock()
	q.concurrency = n
	q.concurrencyMtx.Unlock()
}

// setConcurrencyByPerReplica updates the total concurrency and per replica value
func (q *Queue) setConcurrencyByPerReplica(newPerReplica uint32) {
	q.concurrencyMtx.Lock()
	defer q.concurrencyMtx.Unlock()

	if q.concurrencyPerReplica == newPerReplica {
		return
	}

	replicas := q.concurrency / int(q.concurrencyPerReplica)
	q.concurrency = replicas * int(newPerReplica)
	q.concurrencyPerReplica = newPerReplica
	return
}

// TotalCount returns all requests that have made a call to EnqueueAndWait()
// but have not yet completed.
func (q *Queue) TotalCount() int64 {
	return q.totalCount.Load()
}

// InProgressCount returns all requests that have been dequeued
// but have not yet completed.
func (q *Queue) InProgressCount() int64 {
	return q.inProgressCount.Load()
}
