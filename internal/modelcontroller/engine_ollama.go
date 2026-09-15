package modelcontroller

import (
	"sort"

	kubeaiv1 "github.com/kubeai-project/kubeai/api/k8s/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func (r *ModelReconciler) oLlamaPodForModel(m *kubeaiv1.Model, c ModelConfig) *corev1.Pod {
	lbs := labelsForModel(m)
	ann := r.annotationsForModel(m)

	if _, ok := ann[kubeaiv1.ModelPodPortAnnotation]; !ok {
		// Set port to 8000 (vLLM) if not overwritten.
		ann[kubeaiv1.ModelPodPortAnnotation] = "8000"
	}

	env := []corev1.EnvVar{
		{
			Name:  "OLLAMA_HOST",
			Value: "0.0.0.0:8000",
		},
		{
			// Ollama server typically operates in a 1:N server-to-model mode so it
			// swaps models in and out of memory. In our case we are deploying 1:1
			// model-to-server-pod so we want to always keep the model in memory.
			Name: "OLLAMA_KEEP_ALIVE",
			// Ollama treates 0 as "no keep alive" so we need to set a large value.
			Value: "999999h",
		},
	}
	// Adding an override variable for model location
	if c.Source.url.scheme == "pvc" {
		env = append(env, corev1.EnvVar{
			Name:  "OLLAMA_MODELS",
			Value: "/model",
		})
	}

	var envKeys []string
	for key := range m.Spec.Env {
		envKeys = append(envKeys, key)
	}
	sort.Strings(envKeys)
	for _, key := range envKeys {
		env = append(env, corev1.EnvVar{
			Name:  key,
			Value: m.Spec.Env[key],
		})
	}

	featuresMap := map[kubeaiv1.ModelFeature]struct{}{}
	for _, f := range m.Spec.Features {
		featuresMap[f] = struct{}{}
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   m.Namespace,
			Labels:      lbs,
			Annotations: ann,
		},
		Spec: corev1.PodSpec{
			NodeSelector:       c.NodeSelector,
			Affinity:           c.Affinity,
			Tolerations:        c.Tolerations,
			SchedulerName:      c.SchedulerName,
			RuntimeClassName:   c.RuntimeClassName,
			PriorityClassName:  m.Spec.PriorityClassName,
			ServiceAccountName: r.ModelServerPods.ModelServiceAccountName,
			SecurityContext:    r.ModelServerPods.ModelPodSecurityContext,
			ImagePullSecrets:   r.ModelServerPods.ImagePullSecrets,
			Containers: []corev1.Container{
				{
					Name:            serverContainerName,
					Image:           c.Image,
					Args:            m.Spec.Args,
					Env:             env,
					SecurityContext: r.ModelServerPods.ModelContainerSecurityContext,
					Resources: corev1.ResourceRequirements{
						Requests: c.Requests,
						Limits:   c.Limits,
					},
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: 8000,
							Protocol:      corev1.ProtocolTCP,
							Name:          "http",
						},
					},
					// Use a startup probe to pull the model because ollama server needs
					// to be running already (`ollama pull` issues a HTTP request to the server).
					// Example log from ollama server when a model is pulled:
					// [GIN] 2024/08/20 - 15:12:28 | 200 |  981.561436ms |       127.0.0.1 | POST     "/api/pull"
					StartupProbe: &corev1.Probe{
						InitialDelaySeconds: 1,
						PeriodSeconds:       3,
						FailureThreshold:    10,
						// Give the model pull 180 minutes to complete.
						TimeoutSeconds: 60 * 180,
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{
								Command: ollamaStartupProbeCommand(m, c.Source.url),
							},
						},
					},
					ReadinessProbe: &corev1.Probe{
						FailureThreshold: 3,
						// Will be delayed by the startup probe, so no need to delay here.
						InitialDelaySeconds: 0,
						PeriodSeconds:       10,
						TimeoutSeconds:      2,
						SuccessThreshold:    1,
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/",
								Port: intstr.FromString("http"),
							},
						},
					},
					LivenessProbe: &corev1.Probe{
						FailureThreshold:    3,
						InitialDelaySeconds: 900,
						TimeoutSeconds:      3,
						PeriodSeconds:       30,
						SuccessThreshold:    1,
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/",
								Port: intstr.FromString("http"),
							},
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "dshm",
							MountPath: "/dev/shm",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "dshm",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{
							Medium: corev1.StorageMediumMemory,
							// TODO: Set size limit
						},
					},
				},
			},
		},
	}

	patchFileVolumes(&pod.Spec, m)
	patchServerCacheVolumes(&pod.Spec, m, c)
	c.Source.modelSourcePodAdditions.applyToPodSpec(&pod.Spec, 0)

	return pod

}

func ollamaStartupProbeCommand(m *kubeaiv1.Model, u modelURL) []string {
	// Pull the model and create an alias matching Model.metadata.name.
	// Ollama cp creates a manifest pointing to the existing model data:
	// https://github.com/ollama/ollama/issues/5914
	// Keep model references out of the shell program. bash -c takes a single
	// script followed by $0 and positional arguments; separate command words
	// after -c would only execute /bin/ollama with no arguments.
	script := ""
	source := u.ref
	if u.scheme == "pvc" {
		source = u.modelParam
	} else if u.pull {
		if u.insecure {
			script = `/bin/ollama pull --insecure -- "$1" && `
		} else {
			script = `/bin/ollama pull -- "$1" && `
		}
	}
	// -- keeps a leading-hyphen reference from being interpreted as a CLI flag.
	// && propagates each failure and prevents later preparation steps running.
	script += `/bin/ollama cp -- "$1" "$2"`
	for _, feature := range m.Spec.Features {
		if feature == kubeaiv1.ModelFeatureTextGeneration {
			// Preload before Ready. Embedding-only models do not support run.
			script += ` && /bin/ollama run -- "$2" hi`
			break
		}
	}
	return []string{"/bin/bash", "-c", script, "ollama-startup", source, m.Name}
}
