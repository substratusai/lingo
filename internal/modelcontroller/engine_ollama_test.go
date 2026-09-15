package modelcontroller

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	kubeaiv1 "github.com/kubeai-project/kubeai/api/k8s/v1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOllamaStartupProbeExec(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		url  string
		ref  string
		pull []string
	}{
		"registry":                {"ollama://qwen:0.5b", "qwen:0.5b", []string{"pull", "--", "qwen:0.5b"}},
		"registry-port-path-tag":  {"ollama://registry.example:5000/team/model:v1", "registry.example:5000/team/model:v1", []string{"pull", "--", "registry.example:5000/team/model:v1"}},
		"insecure":                {"ollama://qwen:0.5b?insecure=true", "qwen:0.5b", []string{"pull", "--insecure", "--", "qwen:0.5b"}},
		"no-pull":                 {"ollama://qwen:0.5b?pull=false", "qwen:0.5b", nil},
		"no-pull-insecure":        {"ollama://qwen:0.5b?pull=false&insecure=true", "qwen:0.5b", nil},
		"pvc":                     {"pvc://models?model=qwen:0.5b", "qwen:0.5b", nil},
		"pvc-path-query-decoding": {"pvc://models/subdir?model=team%2Fqwen%3A0.5b&insecure=true&pull=false", "team/qwen:0.5b", nil},
		// The parser permits these references. They must reach Ollama as model
		// arguments, never flags (in particular --help, which would exit zero).
		"leading-hyphen":     {"ollama://--help", "--help", []string{"pull", "--", "--help"}},
		"pvc-leading-hyphen": {"pvc://models?model=--help", "--help", nil},
	}
	features := map[string][]kubeaiv1.ModelFeature{
		"generation": {kubeaiv1.ModelFeatureTextGeneration},
		"embedding":  {kubeaiv1.ModelFeatureTextEmbedding},
		"both":       {kubeaiv1.ModelFeatureTextEmbedding, kubeaiv1.ModelFeatureTextGeneration},
		"none":       {},
	}
	for name, tc := range cases {
		for featureName, fs := range features {
			t.Run(name+"/"+featureName, func(t *testing.T) {
				t.Parallel()
				m := &kubeaiv1.Model{
					ObjectMeta: metav1.ObjectMeta{Name: "model-name", Namespace: "default"},
					Spec:       kubeaiv1.ModelSpec{URL: tc.url, Engine: "OLlama", Features: fs},
				}
				r := &ModelReconciler{}
				src, err := r.parseModelSource(m.Spec.URL)
				require.NoError(t, err)
				pod := r.oLlamaPodForModel(m, ModelConfig{Source: src})
				container := pod.Spec.Containers[0]
				require.Empty(t, container.Command)
				probe := container.StartupProbe
				require.EqualValues(t, 1, probe.InitialDelaySeconds)
				require.EqualValues(t, 3, probe.PeriodSeconds)
				require.EqualValues(t, 10, probe.FailureThreshold)
				require.EqualValues(t, 60*180, probe.TimeoutSeconds)

				var want [][]string
				if tc.pull != nil {
					want = append(want, tc.pull)
				}
				want = append(want, []string{"cp", "--", tc.ref, m.Name})
				if featureName == "generation" || featureName == "both" {
					want = append(want, []string{"run", "--", m.Name, "hi"})
				}

				// Exercise the actual Pod's vector with native Bash, including a
				// repeated successful probe and failures at every preparation step.
				for attempt := 0; attempt < 2; attempt++ {
					got, err := executeOllamaProbe(t, probe.Exec.Command, "")
					require.NoError(t, err)
					require.Equal(t, want, got)
				}
				for i, call := range want {
					got, err := executeOllamaProbe(t, probe.Exec.Command, call[0])
					var exitErr *exec.ExitError
					require.ErrorAs(t, err, &exitErr)
					require.Equal(t, 42, exitErr.ExitCode())
					require.Equal(t, want[:i+1], got, "commands after a failed step must not run")
				}
			})
		}
	}
}

// executeOllamaProbe substitutes only /bin/ollama in the Pod's Bash script
// with a temporary argv logger. It neither modifies /bin nor injects a test
// binary into production. This tests shell execution, not Ollama's API/runtime.
func executeOllamaProbe(t *testing.T, command []string, failAt string) ([][]string, error) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "fake ollama")
	logFile := filepath.Join(dir, "argv")
	require.NoError(t, os.WriteFile(binary, []byte(`#!/bin/bash
printf '%s\0' "$#" "$@" >> "$OLLAMA_TEST_LOG"
if [ -n "$OLLAMA_TEST_FAIL" ] && [ "${1:-}" = "$OLLAMA_TEST_FAIL" ]; then
    exit 42
fi
`), 0700))
	require.GreaterOrEqual(t, len(command), 3)
	require.Equal(t, []string{"/bin/bash", "-c"}, command[:2])
	command = append([]string(nil), command...)
	command[2] = strings.ReplaceAll(command[2], "/bin/ollama", "'"+strings.ReplaceAll(binary, "'", "'\\''")+"'")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Env = append(os.Environ(), "OLLAMA_TEST_LOG="+logFile, "OLLAMA_TEST_FAIL="+failAt)
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), "probe timed out: %s", output)
	data, readErr := os.ReadFile(logFile)
	require.NoError(t, readErr, "probe output: %s", output)
	fields := strings.Split(strings.TrimSuffix(string(data), "\x00"), "\x00")
	var calls [][]string
	for len(fields) > 0 {
		n, parseErr := strconv.Atoi(fields[0])
		require.NoError(t, parseErr)
		require.GreaterOrEqual(t, len(fields), n+1)
		calls = append(calls, append([]string{}, fields[1:n+1]...))
		fields = fields[n+1:]
	}
	return calls, err
}
