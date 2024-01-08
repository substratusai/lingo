package endpoints

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

func newEndpointGroup() *endpointGroup {
	e := &endpointGroup{}
	e.ports = make(map[string]int32)
	e.endpoints = make(map[string]endpoint)
	e.bcast = make(chan struct{})
	return e
}

type endpoint struct {
	inFlight *atomic.Int64
}

type endpointGroup struct {
	mtx       sync.RWMutex
	ports     map[string]int32
	endpoints map[string]endpoint

	bmtx  sync.RWMutex
	bcast chan struct{} // closed when there's a broadcast
}

// getBestHost returns the best host for the given port name. It blocks until there are available endpoints
// in the endpoint group.
//
// It selects the host with the minimum in-flight requests among all the available endpoints.
// The host is returned as a string in the format "IP:Port".
//
// Parameters:
// - portName: The name of the port for which the best host needs to be determined.
//
// Returns:
// - string: The best host with the minimum in-flight requests.
func (e *endpointGroup) getBestHost(ctx context.Context, portName string) (string, error) {
	e.mtx.RLock()
	// await endpoints exists
	for len(e.endpoints) == 0 {
		e.mtx.RUnlock()
		select {
		case <-e.awaitEndpoints():
		case <-ctx.Done():
			return "", ctx.Err()
		}
		e.mtx.RLock()
	}
	var bestIP string
	port := e.getPort(portName)
	var minInFlight int
	for ip := range e.endpoints {
		inFlight := int(e.endpoints[ip].inFlight.Load())
		if bestIP == "" || inFlight < minInFlight {
			bestIP = ip
			minInFlight = inFlight
		}
	}
	e.mtx.RUnlock()
	return fmt.Sprintf("%s:%v", bestIP, port), nil
}

func (e *endpointGroup) awaitEndpoints() chan struct{} {
	e.bmtx.RLock()
	defer e.bmtx.RUnlock()
	return e.bcast
}

func (e *endpointGroup) getAllHosts(portName string) []string {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	var hosts []string
	port := e.getPort(portName)
	for ip := range e.endpoints {
		hosts = append(hosts, fmt.Sprintf("%s:%v", ip, port))
	}

	return hosts
}

func (e *endpointGroup) getPort(portName string) int32 {
	if len(e.ports) == 1 {
		for _, p := range e.ports {
			return p
		}
	}
	return e.ports[portName]
}

func (g *endpointGroup) lenIPs() int {
	g.mtx.RLock()
	defer g.mtx.RUnlock()
	return len(g.endpoints)
}

func (g *endpointGroup) setIPs(ips map[string]struct{}, ports map[string]int32) {
	g.mtx.Lock()
	g.ports = ports
	for ip := range ips {
		if _, ok := g.endpoints[ip]; !ok {
			g.endpoints[ip] = endpoint{inFlight: &atomic.Int64{}}
		}
	}
	for ip := range g.endpoints {
		if _, ok := ips[ip]; !ok {
			delete(g.endpoints, ip)
		}
	}
	g.mtx.Unlock()

	// notify waiting requests
	if len(ips) > 0 {
		g.broadcastEndpoints()
	}
}

func (g *endpointGroup) broadcastEndpoints() {
	g.bmtx.Lock()
	defer g.bmtx.Unlock()

	close(g.bcast)
	g.bcast = make(chan struct{})
}
