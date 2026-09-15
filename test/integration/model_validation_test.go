package integration

import (
	"context"
	"fmt"
	"testing"

	"log"

	v1 "github.com/kubeai-project/kubeai/api/k8s/v1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestModelValidation(t *testing.T) {
	metadata := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Name:      name,
			Namespace: testNS,
		}

	}
	type testCase struct {
		model          v1.Model
		update         func(*v1.Model)
		expValid       bool
		expErrContain  string
		expErrContains []string
	}
	cases := []testCase{
		{
			model: v1.Model{
				ObjectMeta: metadata("empty-invalid"),
				Spec:       v1.ModelSpec{},
			},
			expErrContain: "Required value",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("missing-url-invalid"),
				Spec: v1.ModelSpec{
					Engine:   "Infinity",
					Features: []v1.ModelFeature{},
				},
			},
			expErrContain: "url",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("malformed-url-invalid"),
				Spec: v1.ModelSpec{
					URL:      "not-a-url",
					Engine:   "Infinity",
					Features: []v1.ModelFeature{},
				},
			},
			expErrContain: "url",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("missing-engine-invalid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Features: []v1.ModelFeature{},
				},
			},
			expErrContain: "spec.engine",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("minimum-valid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("a-name-that-has-12345-numbers-valid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("a-model-name-with-40-characters-is-valid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("a-model-name-with-str-len-41-char-invalid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
				},
			},
			expValid: false,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("adapters-valid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
					Adapters: []v1.Adapter{
						{Name: "adapter1", URL: "hf://test-repo/test-adapter"},
					},
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("adapters-supported-engine"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "FasterWhisper",
					Features: []v1.ModelFeature{},
					Adapters: []v1.Adapter{
						{Name: "adapter1", URL: "hf://test-repo/test-adapter"},
					},
				},
			},
			expValid: false,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("invalid-engine"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "NotAValidEngine",
					Features: []v1.ModelFeature{},
				},
			},
			expErrContain: "NotAValidEngine",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("invalid-feature"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{"NotAValidFeature"},
				},
			},
			expErrContain: "NotAValidFeature",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("replicas-0-1-2-valid"),
				Spec: v1.ModelSpec{
					URL:         "hf://test-repo/test-model",
					Engine:      "VLLM",
					Features:    []v1.ModelFeature{},
					MinReplicas: 0,
					Replicas:    ptr.To[int32](1),
					MaxReplicas: ptr.To[int32](2),
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("replicas-1-nil-2-valid"),
				Spec: v1.ModelSpec{
					URL:         "hf://test-repo/test-model",
					Engine:      "VLLM",
					Features:    []v1.ModelFeature{},
					MinReplicas: 1,
					Replicas:    nil,
					MaxReplicas: ptr.To[int32](2),
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("replicas-1-2-nil-valid"),
				Spec: v1.ModelSpec{
					URL:         "hf://test-repo/test-model",
					Engine:      "VLLM",
					Features:    []v1.ModelFeature{},
					MinReplicas: 1,
					Replicas:    ptr.To[int32](2),
					MaxReplicas: nil,
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("replicas-3-nil-2-invalid"),
				Spec: v1.ModelSpec{
					URL:         "hf://test-repo/test-model",
					Engine:      "VLLM",
					Features:    []v1.ModelFeature{},
					MinReplicas: 3,
					MaxReplicas: ptr.To[int32](2),
				},
			},
			expErrContain: "minReplicas should be less than or equal to maxReplicas",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("cache-profile-with-hf-url-valid"),
				Spec: v1.ModelSpec{
					URL:          "hf://test-repo/test-model",
					Engine:       "VLLM",
					Features:     []v1.ModelFeature{},
					CacheProfile: "some-cache-profile",
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("cache-profile-with-s3-url-valid"),
				Spec: v1.ModelSpec{
					URL:          "s3://test-bucket/test-path",
					Engine:       "VLLM",
					Features:     []v1.ModelFeature{},
					CacheProfile: "some-cache-profile",
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("s3-url-without-cache-profile-valid"),
				Spec: v1.ModelSpec{
					URL:      "s3://test-bucket/test-path",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("cache-profile-with-non-hf-url-invalid"),
				Spec: v1.ModelSpec{
					URL:          "ollama://test-repo/test-model",
					Engine:       "VLLM",
					Features:     []v1.ModelFeature{},
					CacheProfile: "some-cache-profile",
				},
			},
			expErrContains: []string{
				"cacheProfile is only supported with urls of format",
				"hf://",
				"s3://",
			},
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("cache-profile-with-non-pvc-url-invalid"),
				Spec: v1.ModelSpec{
					URL:          "pvc://test-pvc/test-model",
					Engine:       "VLLM",
					Features:     []v1.ModelFeature{},
					CacheProfile: "some-cache-profile",
				},
			},
			expErrContains: []string{
				"cacheProfile is only supported with urls of format",
				"hf://",
				"s3://",
			},
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("update-no-changes-valid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
				},
			},
			update:   func(m *v1.Model) { /* No changes */ },
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("mutate-cacheprofile-invalid"),
				Spec: v1.ModelSpec{
					URL:          "hf://test-repo/test-model",
					Engine:       "VLLM",
					Features:     []v1.ModelFeature{},
					CacheProfile: "some-cache-profile",
				},
			},
			update: func(m *v1.Model) {
				m.Spec.CacheProfile = "some-updated-cache-profile"
			},
			expErrContain: "cacheProfile is immutable",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("url-mutable-without-cache-profile-valid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
				},
			},
			update: func(m *v1.Model) {
				m.Spec.URL = "hf://test-repo/updated-model"
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("url-immutable-with-cache-profile-invalid"),
				Spec: v1.ModelSpec{
					URL:          "hf://test-repo/test-model",
					Engine:       "VLLM",
					Features:     []v1.ModelFeature{},
					CacheProfile: "some-cache-profile",
				},
			},
			update: func(m *v1.Model) {
				m.Spec.URL = "hf://test-repo/updated-model"
			},
			expErrContain: "url is immutable when using cacheProfile",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("root-file-path-valid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
					Files: []v1.File{
						{
							Path:    "/file.txt",
							Content: "file content",
						},
					},
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("absolute-file-path-valid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
					Files: []v1.File{
						{
							Path:    "/absolute/path/to/file.txt",
							Content: "file content",
						},
					},
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("relative-file-path-invalid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
					Files: []v1.File{
						{
							Path:    "relative/path/to/file.txt",
							Content: "file content",
						},
					},
				},
			},
			expErrContain: "Path must be an absolute path",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("invalid-file-path-character"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
					Files: []v1.File{
						{
							Path:    "c://path/to/file.txt",
							Content: "file content",
						},
					},
				},
			},
			expErrContain: "must not contain a ':' character",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("duplicate-file-paths-invalid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
					Files: []v1.File{
						{
							Path:    "/path/to/file1.txt",
							Content: "file 1 content",
						},
						{
							Path:    "/path/to/file2.txt",
							Content: "file 2 content",
						},
						{
							Path:    "/path/to/file1.txt", // Duplicate path
							Content: "duplicated path content",
						},
					},
				},
			},
			expErrContain: "All file paths must be unique",
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("large-files-valid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
					Files: []v1.File{
						{
							Path:    "/path/to/file1.txt",
							Content: nCharacters(100_000),
						},
					},
				},
			},
			expValid: true,
		},
		{
			model: v1.Model{
				ObjectMeta: metadata("xlarge-files-invalid"),
				Spec: v1.ModelSpec{
					URL:      "hf://test-repo/test-model",
					Engine:   "VLLM",
					Features: []v1.ModelFeature{},
					Files: []v1.File{
						{
							Path:    "/path/to/file1.txt",
							Content: nCharacters(100_001),
						},
					},
				},
			},
			expErrContain: "may not be more than 100000 bytes",
		},
	}
	// The CRD rejects shell syntax in references before reconciliation. Query
	// model parameters are decoded and checked separately by parseModelURL.
	for i, ref := range []string{
		"model;id", "model&&id", "model|id", "$(id)", "`id`",
		"model name", "model\nname", "model%3Bid", "%24%28id%29", "model%0Aname",
	} {
		cases = append(cases, testCase{
			model: v1.Model{
				ObjectMeta: metadata(fmt.Sprintf("shell-reference-%d", i)),
				Spec:       v1.ModelSpec{URL: "ollama://" + ref, Engine: "OLlama", Features: []v1.ModelFeature{}},
			},
			expErrContain: "spec.url",
		})
	}
	for _, c := range cases {
		t.Run(c.model.Name, func(t *testing.T) {
			t.Parallel()

			validateErr := func(err error) {
				if c.expValid {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					if c.expErrContain != "" {
						require.Contains(t, err.Error(), c.expErrContain)
					}
					for _, expErrContain := range c.expErrContains {
						require.Contains(t, err.Error(), expErrContain)
					}
				}
			}

			if c.expValid {
				t.Cleanup(func() {
					if err := testK8sClient.Delete(context.Background(), &c.model); err != nil {
						log.Printf("Failed to delete model: %v", err)
					}
				})
			}
			if c.update == nil {
				validateErr(testK8sClient.Create(testCtx, &c.model))
			} else {
				require.NoError(t, testK8sClient.Create(testCtx, &c.model))
				c.update(&c.model)
				validateErr(testK8sClient.Update(testCtx, &c.model))
			}
		})
	}
}

func nCharacters(n int) string {
	s := make([]byte, n)
	for i := range s {
		s[i] = 'a'
	}
	return string(s)
}
