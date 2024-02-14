package deployments

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rtfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReadinessChecker(t *testing.T) {
	tests := map[string]struct {
		bootstrapped bool
		expectError  bool
	}{
		"not_bootstrapped": {
			expectError: true,
		},
		"bootstrapped": {
			bootstrapped: true,
			expectError:  false,
		},
	}
	for name, spec := range tests {
		t.Run(name, func(t *testing.T) {
			mgr := &Manager{
				Client: rtfake.NewClientBuilder().Build(),
			}
			if spec.bootstrapped {
				require.NoError(t, mgr.Bootstrap(context.TODO()))
			}
			// when
			gotErr := mgr.ReadinessChecker(nil)
			if spec.expectError {

				assert.Error(t, gotErr)
				return
			}
			assert.NoError(t, gotErr)
		})
	}
}

func TestAddDeployment(t *testing.T) {
	specs := map[string]struct {
		deployment    appsv1.Deployment
		expScale      scale
		expectedError error
		expModels     []string
	}{
		"single model - default replica settings": {
			deployment: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-deployment",
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								lingoDomain + "/models": "my-model1",
							},
						},
					},
				},
			},
			expModels: []string{"my-model1"},
			expScale:  scale{Current: 3, Min: 0, Max: 3},
		},
		"single model - annotated": {
			deployment: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-deployment",
					Annotations: map[string]string{
						lingoDomain + "/min-replicas": "2",
						lingoDomain + "/max-replicas": "5",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								lingoDomain + "/models": "my-model1",
							},
						},
					},
				},
			},
			expModels: []string{"my-model1"},
			expScale:  scale{Current: 3, Min: 2, Max: 5},
		},
		"multi model": {
			deployment: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-deployment",
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								lingoDomain + "/models": "my-model1,my-model2",
							},
						},
					},
				},
			},
			expModels: []string{"my-model1", "my-model2"},
			expScale:  scale{Current: 3, Min: 0, Max: 3},
		},
		"no model - skipped": {
			deployment: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "my-deployment",
					Annotations: map[string]string{},
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{},
					},
				},
			},
		},
	}
	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			depScale := &autoscalingv1.Scale{
				Spec: autoscalingv1.ScaleSpec{
					Replicas: 3,
				},
			}

			r := &Manager{
				Client:            &partialFakeClient{subRes: depScale},
				Namespace:         "default",
				modelToDeployment: make(map[string]string),
				scalers:           map[string]*scaler{},
			}

			// when
			gotErr := r.addDeployment(context.Background(), spec.deployment)

			// then
			require.NoError(t, gotErr)

			for _, v := range spec.expModels {
				dep, ok := r.ResolveDeployment(v)
				require.True(t, ok)
				assert.Equal(t, "my-deployment", dep)
			}
			assert.Len(t, r.modelToDeployment, len(spec.expModels))
			scales := r.getScalesSnapshot()
			assert.Equal(t, spec.expScale, scales["my-deployment"])
		})
	}
}

func TestRemoveDeployment(t *testing.T) {
	const myDeployment = "myDeployment"
	specs := map[string]struct {
		setup      func(t *testing.T, m *Manager)
		expScalers map[string]scale
	}{
		"single model deployment": {
			setup: func(t *testing.T, m *Manager) {
				m.setModelMapping("model1", myDeployment)
				m.getScaler(myDeployment)
			},
			expScalers: map[string]scale{},
		},
		"multi model deployment": {
			setup: func(t *testing.T, m *Manager) {
				m.setModelMapping("model1", myDeployment)
				m.setModelMapping("model2", myDeployment)
				m.setModelMapping("other", "other")
				m.getScaler(myDeployment)
				m.getScaler("other")
			},
			expScalers: map[string]scale{"other": {Current: -1}},
		},
		"unknown deployment - ignored": {
			setup: func(t *testing.T, m *Manager) {
				m.setModelMapping("other", "other")
				m.getScaler("other")
			},
			expScalers: map[string]scale{"other": {Current: -1}},
		},
	}
	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			m := &Manager{
				scalers:           make(map[string]*scaler),
				modelToDeployment: make(map[string]string),
			}
			spec.setup(t, m)
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: myDeployment}}
			// when
			m.removeDeployment(req)
			// then
			_, exists := m.ResolveDeployment(myDeployment)
			assert.False(t, exists)
			assert.Equal(t, spec.expScalers, m.getScalesSnapshot())
		})
	}
}

type partialFakeClient struct {
	client.Client
	subRes client.Object
}

func (f *partialFakeClient) SubResource(subResource string) client.SubResourceClient {
	return &partialSubResFakeClient{sourceSubRes: &f.subRes}
}

type partialSubResFakeClient struct {
	client.SubResourceClient
	sourceSubRes *client.Object
}

func (f *partialSubResFakeClient) Get(ctx context.Context, obj client.Object, target client.Object, opts ...client.SubResourceGetOption) error {
	reflect.ValueOf(target).Elem().Set(reflect.ValueOf(*f.sourceSubRes).Elem())
	return nil
}
