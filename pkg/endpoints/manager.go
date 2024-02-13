package endpoints

import (
	"context"
	"fmt"
	"log"
	"sync"

	disv1 "k8s.io/api/discovery/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewManager(mgr ctrl.Manager) (*Manager, error) {
	r := &Manager{}
	r.Client = mgr.GetClient()
	r.endpoints = map[string]*endpointGroup{}
	r.ExcludePods = map[string]struct{}{}
	if err := r.SetupWithManager(mgr); err != nil {
		return nil, err
	}
	return r, nil
}

type Manager struct {
	client.Client

	EndpointSizeCallback func(deploymentName string, size int)

	endpointsMtx sync.Mutex
	endpoints    map[string]*endpointGroup

	ExcludePods map[string]struct{}
}

func (r *Manager) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&disv1.EndpointSlice{}).
		Complete(r)
}

func (r *Manager) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var slice disv1.EndpointSlice
	if err := r.Get(ctx, req.NamespacedName, &slice); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	const serviceNameLabel = "kubernetes.io/service-name"
	serviceName, ok := slice.GetLabels()[serviceNameLabel]
	if !ok {
		log.Printf("no service label on endpointslice: %v", req.Name)
		return ctrl.Result{}, nil
	}

	var sliceList disv1.EndpointSliceList
	if err := r.List(ctx, &sliceList, client.MatchingLabels{serviceNameLabel: serviceName}); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing endpointslices: %w", err)
	}

	ips := map[string]struct{}{}
	for _, sliceItem := range sliceList.Items {
		for _, endpointItem := range sliceItem.Endpoints {
			if endpointItem.TargetRef != nil && endpointItem.TargetRef.Kind == "Pod" {
				if _, ok := r.ExcludePods[endpointItem.TargetRef.Name]; ok {
					continue
				}
			}
			ready := endpointItem.Conditions.Ready
			if ready != nil && *ready {
				for _, ip := range endpointItem.Addresses {
					ips[ip] = struct{}{}
				}
			}
		}
	}

	ports := map[string]int32{}
	for _, p := range slice.Ports {
		var name string
		if p.Name != nil {
			name = *p.Name
		}
		if p.Port != nil {
			ports[name] = *p.Port
		}
	}

	priorLen := r.getEndpoints(serviceName).lenIPs()
	r.getEndpoints(serviceName).setIPs(ips, ports)

	if priorLen != len(ips) {
		// TODO: Currently Service name needs to match Deployment name, however
		// this shouldn't be the case. We should be able to reference deployment
		// replicas by something else.
		r.EndpointSizeCallback(serviceName, len(ips))
	}

	return ctrl.Result{}, nil
}

func (r *Manager) getEndpoints(service string) *endpointGroup {
	r.endpointsMtx.Lock()
	e, ok := r.endpoints[service]
	if !ok {
		e = newEndpointGroup()
		r.endpoints[service] = e
	}
	r.endpointsMtx.Unlock()
	return e
}

// AwaitHostAddress returns the host address with the lowest number of in-flight requests. It will block until the host address
// becomes available or the context times out.
//
// It returns a string in the format "host:port" or error on timeout.
func (r *Manager) AwaitHostAddress(ctx context.Context, service, portName string) (string, error) {
	return r.getEndpoints(service).getBestHost(ctx, portName)
}

// GetAllHosts retrieves the list of all hosts for a given service and port.
func (r *Manager) GetAllHosts(service, portName string) []string {
	return r.getEndpoints(service).getAllHosts(portName)
}
