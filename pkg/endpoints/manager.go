package endpoints

import (
	"context"
	"fmt"
	"log"
	"sync"

	corev1 "k8s.io/api/core/v1"
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

	deployments := map[string]map[string]struct{}{}

	ips := map[string]struct{}{}
	for _, sliceItem := range sliceList.Items {
		for _, endpointItem := range sliceItem.Endpoints {
			if endpointItem.TargetRef != nil && endpointItem.TargetRef.Kind == "Pod" {
				if _, ok := r.ExcludePods[endpointItem.TargetRef.Name]; ok {
					continue
				}
			}
			ready := endpointItem.Conditions.Ready
			if ready != nil && *ready && endpointItem.TargetRef.Kind == "Pod" {
				podName := endpointItem.TargetRef.Name
				namespace := endpointItem.TargetRef.Namespace // Assuming the EndpointSlice and Pod are in the same namespace
				// Fetch the Pod using the client
				var pod corev1.Pod
				if err := r.Get(ctx, client.ObjectKey{Name: podName, Namespace: namespace}, &pod); err != nil {
					log.Printf("error fetching pod: %v\n", err)
					continue
				}
				if pod.OwnerReferences != nil {
					for _, owner := range pod.OwnerReferences {
						if owner.Kind == "ReplicaSet" {
							for _, ip := range endpointItem.Addresses {
								ips[ip] = struct{}{}
							}
							deployments[owner.Name] = ips
						}
					}
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

	for deploymentName, ips := range deployments {
		priorLen := r.getEndpoints(deploymentName).lenIPs()
		r.getEndpoints(deploymentName).setIPs(ips, ports)

		if priorLen != len(ips) {
			r.EndpointSizeCallback(deploymentName, len(ips))
		}

	}

	return ctrl.Result{}, nil
}

func (r *Manager) getEndpoints(deploymentName string) *endpointGroup {
	r.endpointsMtx.Lock()
	e, ok := r.endpoints[deploymentName]
	if !ok {
		e = newEndpointGroup()
		r.endpoints[deploymentName] = e
	}
	r.endpointsMtx.Unlock()
	return e
}

// AwaitHostAddress returns the host address with the lowest number of in-flight requests. It will block until the host address
// becomes available or the context times out.
//
// It returns a string in the format "host:port" or error on timeout
func (r *Manager) AwaitHostAddress(ctx context.Context, deploymentName, portName string) (string, error) {
	return r.getEndpoints(deploymentName).getBestHost(ctx, portName)
}

// GetAllHosts retrieves the list of all hosts for a given service and port.
func (r *Manager) GetAllHosts(deploymentName, portName string) []string {
	return r.getEndpoints(deploymentName).getAllHosts(portName)
}
