package queue

import (
	"context"
	"log"
	"sync"
)

func NewManager(concurrencyPerReplica int) *Manager {
	m := &Manager{}
	m.queues = map[string]*Queue{}
	m.concurrencyPerReplica = concurrencyPerReplica
	return m
}

// Manager manages the a set of Queues (for Deployments).
type Manager struct {
	// concurrencyPerReplica of each queue for each deployment replica.
	concurrencyPerReplica int

	mtx    sync.RWMutex
	queues map[string]*Queue // deployment to queue
}

// TotalCounts returns the number of pending or in-progress requests for each deployment name.
func (m *Manager) TotalCounts() map[string]int64 {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	sizes := make(map[string]int64, len(m.queues))
	for name, q := range m.queues {
		sizes[name] = q.TotalCount()
	}
	return sizes
}

// InProgressCount returns the number of in-progress requests for each deployment name.
func (m *Manager) InProgressCount() map[string]int64 {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	sizes := make(map[string]int64, len(m.queues))
	for name, q := range m.queues {
		sizes[name] = q.InProgressCount()
	}
	return sizes
}

// EnqueueAndWait adds a request to the queue for the given deployment name
// and wait for it to be dequeued.
func (m *Manager) EnqueueAndWait(ctx context.Context, deploymentName, id string) func() {
	return m.getQueue(deploymentName).EnqueueAndWait(ctx, id)
}

// UpdateQueueSizeForReplicas updates the queue size for the given deployment name.
func (m *Manager) UpdateQueueSizeForReplicas(deploymentName string, replicas int) {
	// max is needed to prevent the queue size from being set to 0
	newSize := max(replicas*m.concurrencyPerReplica, m.concurrencyPerReplica)
	log.Printf("Updating queue size: deployment: %v, replicas: %v, newSize: %v", deploymentName, replicas, newSize)
	m.getQueue(deploymentName).SetConcurrency(newSize)
}

// getQueue returns the queue for the given model name.
// if the model does not have a queue, a new queue is created.
func (m *Manager) getQueue(deploymentName string) *Queue {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	q, ok := m.queues[deploymentName]
	if !ok {
		q = New(m.concurrencyPerReplica)
		m.queues[deploymentName] = q
		go q.Start()
	}
	return q
}
