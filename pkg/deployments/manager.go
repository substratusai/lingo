package deployments

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const lingoDomain = "lingo.substratus.ai"

func NewManager(mgr ctrl.Manager) (*Manager, error) {
	r := &Manager{}
	r.Client = mgr.GetClient()
	r.scalers = map[string]*scaler{}
	r.modelToDeployment = map[string]string{}
	if err := r.SetupWithManager(mgr); err != nil {
		return nil, err
	}
	return r, nil
}

type Manager struct {
	client.Client

	Namespace string

	ScaleDownPeriod time.Duration

	scalersMtx sync.Mutex

	// scalers maps deployment names to scalers
	scalers map[string]*scaler

	modelToDeploymentMtx sync.RWMutex

	// modelToDeployment maps model names to deployment names. A single deployment
	// can serve multiple models.
	modelToDeployment map[string]string

	bootstrapped atomic.Bool
}

func (r *Manager) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Deployment{}).
		Complete(r)
}

func (r *Manager) AtLeastOne(deploymentName string) {
	r.getScaler(deploymentName).AtLeastOne()
}

func (r *Manager) SetDesiredScale(deploymentName string, n int32) {
	r.getScaler(deploymentName).SetDesiredScale(n)
}

func (r *Manager) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var d appsv1.Deployment
	switch err := r.Get(ctx, req.NamespacedName, &d); {
	case apierrors.IsNotFound(err):
		r.removeDeployment(req)
		return ctrl.Result{}, nil
	case err != nil:
		return ctrl.Result{}, fmt.Errorf("get: %w", err)
	}

	return ctrl.Result{}, r.addDeployment(ctx, d)
}

func (r *Manager) addDeployment(ctx context.Context, d appsv1.Deployment) error {
	models := getModelsFromAnnotation(d.GetAnnotations())
	log.Printf("deployment: %v models: %v", d.Name, models)
	if len(models) == 0 {
		return nil
	}
	for _, model := range models {
		r.setModelMapping(strings.TrimSpace(model), d.Name)
	}
	var scale autoscalingv1.Scale
	if err := r.SubResource("scale").Get(ctx, &d, &scale); err != nil {
		return fmt.Errorf("get scale: %w", err)
	}

	deploymentName := d.Name

	r.getScaler(deploymentName).UpdateState(
		scale.Spec.Replicas,
		getAnnotationInt32(d.GetAnnotations(), lingoDomain+"/min-replicas", 0),
		getAnnotationInt32(d.GetAnnotations(), lingoDomain+"/max-replicas", 3),
	)

	return nil
}

func (r *Manager) hasModel(d *appsv1.Deployment) bool {
	models := getModelsFromAnnotation(d.GetAnnotations())
	if len(models) == 0 {
		return false
	}
	return true
}

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

func (r *Manager) removeDeployment(req ctrl.Request) {
	r.scalersMtx.Lock()
	delete(r.scalers, req.Name)
	r.scalersMtx.Unlock()

	r.modelToDeploymentMtx.Lock()
	for model, deployment := range r.modelToDeployment {
		if deployment == req.Name {
			delete(r.modelToDeployment, model)
		}
	}
	r.modelToDeploymentMtx.Unlock()
}

func (r *Manager) getScaler(deploymentName string) *scaler {
	r.scalersMtx.Lock()
	b, ok := r.scalers[deploymentName]
	if !ok {
		b = newScaler(r.ScaleDownPeriod, r.scaleFunc(context.TODO(), deploymentName))
		r.scalers[deploymentName] = b
	}
	r.scalersMtx.Unlock()
	return b
}

// getScalesSnapshot returns a snapshot of the stats for all scalers managed by the Manager.
// The scales are returned as a map, where the keys are the model names.
func (r *Manager) getScalesSnapshot() map[string]scale {
	r.scalersMtx.Lock()
	defer r.scalersMtx.Unlock()
	result := make(map[string]scale, len(r.scalers))
	for k, v := range r.scalers {
		result[k] = v.getScale()
	}
	return result
}

func (r *Manager) scaleFunc(ctx context.Context, deploymentName string) func(int32, bool) error {
	return func(n int32, atLeastOne bool) error {
		if atLeastOne {
			log.Printf("Scaling model %q: at-least-one", deploymentName)
		} else {
			log.Printf("Scaling model %q: %v", deploymentName, n)
		}

		req := types.NamespacedName{Namespace: r.Namespace, Name: deploymentName}
		var d appsv1.Deployment
		if err := r.Get(ctx, req, &d); err != nil {
			return fmt.Errorf("get: %w", err)
		}

		if !r.hasModel(&d) {
			return fmt.Errorf("not scaling deployment %q: deployment has no models annotation", deploymentName)
		}

		var scale autoscalingv1.Scale
		if err := r.SubResource("scale").Get(ctx, &d, &scale); err != nil {
			return fmt.Errorf("get scale: %w", err)
		}

		if atLeastOne {
			if scale.Spec.Replicas == 0 {
				scale.Spec.Replicas = 1
				if err := r.SubResource("scale").Update(ctx, &d, client.WithSubResourceBody(&scale)); err != nil {
					return fmt.Errorf("update scale (from zero): %w", err)
				}
			}
			return nil
		}

		if scale.Spec.Replicas != n {
			scale.Spec.Replicas = n
			if err := r.SubResource("scale").Update(ctx, &d, client.WithSubResourceBody(&scale)); err != nil {
				return fmt.Errorf("update scale (desired): %w", err)
			}
		}

		return nil
	}
}

func (r *Manager) setModelMapping(modelName, deploymentName string) {
	r.modelToDeploymentMtx.Lock()
	r.modelToDeployment[modelName] = deploymentName
	r.modelToDeploymentMtx.Unlock()
}

func (r *Manager) ResolveDeployment(model string) (string, bool) {
	r.modelToDeploymentMtx.RLock()
	deploy, ok := r.modelToDeployment[model]
	r.modelToDeploymentMtx.RUnlock()
	return deploy, ok
}

// Bootstrap initializes the Manager by retrieving a list of deployments from the k8s cluster and adding them to the Manager's internal state.
func (r *Manager) Bootstrap(ctx context.Context) error {
	var sliceList appsv1.DeploymentList
	if err := r.List(ctx, &sliceList, client.InNamespace(r.Namespace)); err != nil {
		return fmt.Errorf("list deployments: %w", err)
	}
	for _, d := range sliceList.Items {
		if err := r.addDeployment(ctx, d); err != nil {
			return err
		}
	}
	r.bootstrapped.Store(true)
	return nil
}

// ReadinessChecker checks if the Manager state is loaded and ready to handle requests.
// It returns an error if Manager is not bootstrapped yet.
// To be used with sigs.k8s.io/controller-runtime manager `AddReadyzCheck`.
func (r *Manager) ReadinessChecker(_ *http.Request) error {
	if !r.bootstrapped.Load() {
		return fmt.Errorf("not boostrapped yet")
	}
	return nil
}

func getAnnotationInt32(ann map[string]string, key string, defaultValue int32) int32 {
	if ann == nil {
		return defaultValue
	}

	str, ok := ann[key]
	if !ok {
		return defaultValue
	}

	value, err := strconv.Atoi(str)
	if err != nil {
		log.Printf("parsing annotation as int: %v", err)
		return defaultValue
	}
	if value > math.MaxInt32 {
		log.Printf("invalid value that exceeds max int32: %d", value)
		return defaultValue
	}
	return int32(value) // #nosec G109 : checked before
}
