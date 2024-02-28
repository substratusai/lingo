package endpoints

import (
	"context"
	"fmt"
	"log"
	"sync"

	corev1 "k8s.io/api/core/v1"

	disv1 "k8s.io/api/discovery/v1"

	k8stypes "k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewManager(mgr ctrl.Manager) (*Manager, error) {
	r := &Manager{
		Client:              mgr.GetClient(),
		endpoints:           make(map[string]*endpointGroup),
		serviceToDeployment: make(map[string]string),
		ExcludePods:         make(map[string]struct{}),
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return nil, err
	}
	return r, nil
}

type Manager struct {
	client.Client

	EndpointSizeCallback func(deploymentName string, size int)

	endpointsMtx sync.Mutex
	endpoints    map[string]*endpointGroup // deployment to endpoints

	serviceToDeploymentMtx sync.RWMutex
	serviceToDeployment    map[string]string

	ExcludePods map[string]struct{}
}

func (r *Manager) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&disv1.EndpointSlice{}).
		Complete(ReconcilerFn(r.ReconcileEndpointSlices))
	if err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Service{}).
		Complete(ReconcilerFn(r.ReconcileServices))
}

func (r *Manager) ReconcileServices(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var svc corev1.Service
	if err := r.Get(ctx, req.NamespacedName, &svc); err != nil {
		// todo: handle service undeployment
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	deploymentName := getDeploymentFromAnnotation(svc.Annotations)
	if deploymentName == "" {
		return ctrl.Result{}, nil
	}
	r.serviceToDeploymentMtx.Lock()
	r.serviceToDeployment[req.Name] = deploymentName // todo: should be namespaced names instead
	r.serviceToDeploymentMtx.Unlock()
	return ctrl.Result{}, nil
}

func (r *Manager) ReconcileEndpointSlices(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
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

	r.serviceToDeploymentMtx.RLock()
	deploymentName, ok := r.serviceToDeployment[serviceName]
	r.serviceToDeploymentMtx.RUnlock()
	if !ok {
		// maybe triggered before service, let's reconcile
		if _, err := r.ReconcileServices(ctx, ctrl.Request{k8stypes.NamespacedName{Namespace: req.Namespace, Name: serviceName}}); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		r.serviceToDeploymentMtx.RLock()
		deploymentName, ok = r.serviceToDeployment[serviceName]
		r.serviceToDeploymentMtx.RUnlock()
		if !ok {
			// still not there, then svc is not annotated
			log.Printf("no deployment for service: %q", req.Name)
			return ctrl.Result{}, nil
		}
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

	priorLen := r.getEndpoints(deploymentName).lenIPs()
	r.getEndpoints(deploymentName).setIPs(ips, ports)

	if priorLen != len(ips) {
		// TODO: Currently Service name needs to match Deployment name, however
		// this shouldn't be the case. We should be able to reference deployment
		// replicas by something else.
		r.EndpointSizeCallback(deploymentName, len(ips))
	}

	return ctrl.Result{}, nil
}

func (r *Manager) getEndpoints(deployment string) *endpointGroup {
	r.endpointsMtx.Lock()
	e, ok := r.endpoints[deployment]
	if !ok {
		e = newEndpointGroup()
		r.endpoints[deployment] = e
	}
	r.endpointsMtx.Unlock()
	return e
}

// AwaitHostAddress returns the host address with the lowest number of in-flight requests. It will block until the host address
// becomes available or the context times out.
//
// It returns a string in the format "host:port" or error on timeout
func (r *Manager) AwaitHostAddress(ctx context.Context, deployment, portName string) (string, error) {
	return r.getEndpoints(deployment).getBestHost(ctx, portName)
}

// GetAllHosts retrieves the list of all hosts for a given service and port.
func (r *Manager) GetAllHosts(service, portName string) []string {
	r.serviceToDeploymentMtx.RLock()
	defer r.serviceToDeploymentMtx.RUnlock()
	depName, ok := r.serviceToDeployment[service]
	if !ok {
		return nil
	}
	return r.getEndpoints(depName).getAllHosts(portName)
}

type ReconcilerFn func(ctx context.Context, req ctrl.Request) (ctrl.Result, error)

func (r ReconcilerFn) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return r(ctx, req)
}

const lingoDeploymentAnnotation = "lingo.substratus.ai/deployment"

func getDeploymentFromAnnotation(ann map[string]string) string {
	if len(ann) == 0 {
		return ""
	}
	modelCSV, ok := ann[lingoDeploymentAnnotation]
	if !ok {
		return ""
	}
	return modelCSV
}
