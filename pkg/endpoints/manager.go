package endpoints

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	disv1 "k8s.io/api/discovery/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewManager(mgr ctrl.Manager) (*Manager, error) {
	r := &Manager{
		svcEndpoints: make(map[string]*endpointGroup),
		podEndpoints: make(map[string]*endpointGroup),
		ExcludePods:  make(map[string]struct{}),
		Client:       mgr.GetClient(),
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return nil, err
	}
	return r, nil
}

type Manager struct {
	client.Client

	EndpointSizeCallback func(deploymentName string, size int)

	// service name to endpoints
	svcEndpointsMtx sync.Mutex
	svcEndpoints    map[string]*endpointGroup
	// model to endpoints
	podEndpointsMtx sync.Mutex
	podEndpoints    map[string]*endpointGroup

	ExcludePods map[string]struct{}
}

func (r *Manager) SetupWithManager(mgr ctrl.Manager) error {
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&disv1.EndpointSlice{}).
		Complete(ReconcilerFn(r.ReconcileSVC)); err != nil {
		return ctrl.NewControllerManagedBy(mgr).
			For(&disv1.EndpointSlice{}).
			Complete(ReconcilerFn(r.ReconcileSVC))
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Complete(ReconcilerFn(r.ReconcilePods))
}

func (r *Manager) ReconcilePods(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	switch pod.Status.Phase {
	case corev1.PodPending:
		return ctrl.Result{}, nil
	case corev1.PodRunning:
		// new or updated instance
		ports := make(map[string]int32)
		for _, container := range pod.Spec.Containers {
			for _, port := range container.Ports {
				ports[port.Name] = port.ContainerPort
			}
		}
		for _, m := range getModelsFromAnnotation(pod.Annotations) {
			r.getEndpoints(m).addEndpoint(pod.Status.PodIP, ports)
		}
	case corev1.PodSucceeded, corev1.PodFailed:
		// pod had terminated
		for _, m := range getModelsFromAnnotation(pod.Annotations) {
			r.getEndpoints(m).removeEndpoint(pod.Status.PodIP)
		}
	}

	return ctrl.Result{}, nil
}

const lingoDomain = "lingo.substratus.ai"

func getModelsFromAnnotation(ann map[string]string) []string {
	if len(ann) == 0 {
		return []string{}
	}
	modelCSV, ok := ann[lingoDomain+"/models"]
	if !ok {
		return []string{}
	}
	return strings.Split(modelCSV, ",")
}

func (r *Manager) ReconcileSVC(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if req.Name != "lingo" { // todo: lingo is hard coded
		return ctrl.Result{}, nil
	}
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

	priorLen := r.getSVCEndpoints(serviceName).lenIPs()
	r.getSVCEndpoints(serviceName).setIPs(ips, ports)

	if priorLen != len(ips) {
		// TODO: Currently Service name needs to match Deployment name, however
		// this shouldn't be the case. We should be able to reference deployment
		// replicas by something else.
		r.EndpointSizeCallback(serviceName, len(ips))
	}

	return ctrl.Result{}, nil
}

func (r *Manager) getSVCEndpoints(service string) *endpointGroup {
	r.svcEndpointsMtx.Lock()
	e, ok := r.svcEndpoints[service]
	if !ok {
		e = newEndpointGroup()
		r.svcEndpoints[service] = e
	}
	r.svcEndpointsMtx.Unlock()
	return e
}

func (r *Manager) getEndpoints(model string) *endpointGroup {
	r.podEndpointsMtx.Lock()
	e, ok := r.podEndpoints[model]
	if !ok {
		e = newEndpointGroup()
		r.podEndpoints[model] = e
	}
	r.podEndpointsMtx.Unlock()
	return e
}

// AwaitHostAddress returns the host address with the lowest number of in-flight requests. It will block until the host address
// becomes available or the context times out.
//
// It returns a string in the format "host:port" or error on timeout
func (r *Manager) AwaitHostAddress(ctx context.Context, model, portName string) (string, error) {
	return r.getEndpoints(model).getBestHost(ctx, portName)
}

// GetAllLingoHosts retrieves the list of all hosts for a given service and port.
func (r *Manager) GetAllLingoHosts(portName string) []string {
	// todo: service name is fix to 'lingo'
	return r.getSVCEndpoints("lingo").getAllHosts(portName)
}

type ReconcilerFn func(ctx context.Context, req ctrl.Request) (ctrl.Result, error)

func (r ReconcilerFn) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return r(ctx, req)
}
