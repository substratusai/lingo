package modelcontroller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parseModelURL(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		name    string
		input   string
		want    modelURL
		wantErr bool
	}{
		"empty": {
			input:   "",
			wantErr: true,
		},
		"invalid-scheme": {
			input:   "iNv@lid://path/to/model",
			wantErr: true,
		},
		"double-scheme-edge-case": {
			input: "a://path/b://to/model",
			want: modelURL{
				scheme: "a",
				ref:    "path/b://to/model",
				name:   "path",
				path:   "b://to/model",
				pull:   true,
			},
		},
		"valid-google-storage": {
			input: "gs://bucket-name/path/to/model",
			want: modelURL{
				scheme: "gs",
				ref:    "bucket-name/path/to/model",
				name:   "bucket-name",
				path:   "path/to/model",
				pull:   true,
			},
		},
		"valid-ollama": {
			input: "ollama://gemma2:2b",
			want: modelURL{
				scheme: "ollama",
				ref:    "gemma2:2b",
				name:   "gemma2:2b",
				path:   "",
				pull:   true,
			},
		},
		"valid-huggingface": {
			input: "hf://test-user/model-name",
			want: modelURL{
				scheme: "hf",
				ref:    "test-user/model-name",
				name:   "test-user",
				path:   "model-name",
				pull:   true,
			},
		},
		"valid-s3": {
			input: "s3://test-bucket/model-name",
			want: modelURL{
				scheme: "s3",
				ref:    "test-bucket/model-name",
				name:   "test-bucket",
				path:   "model-name",
				pull:   true,
			},
		},
		"valid-pvc": {
			input: "pvc://my-vpc/path/to/model",
			want: modelURL{
				scheme: "pvc",
				ref:    "my-vpc/path/to/model",
				name:   "my-vpc",
				path:   "path/to/model",
				pull:   true,
			},
		},
		"valid-pvc-no-path": {
			input: "pvc://my-vpc",
			want: modelURL{
				scheme: "pvc",
				ref:    "my-vpc",
				name:   "my-vpc",
				path:   "",
				pull:   true,
			},
		},
		"valid-pvc-with-slash-empty": {
			input: "pvc://my-vpc/",
			want: modelURL{
				scheme: "pvc",
				ref:    "my-vpc/",
				name:   "my-vpc",
				path:   "",
				pull:   true,
			},
		},
		"valid-pvc-with-double-slash": {
			input: "pvc://my-vpc//",
			want: modelURL{
				scheme: "pvc",
				ref:    "my-vpc//",
				name:   "my-vpc",
				path:   "/",
				pull:   true,
			},
		},
		"valid-pvc-with-modelname": {
			input: "pvc://my-vpc?model=qwen2:0.5b",
			want: modelURL{
				scheme:     "pvc",
				ref:        "my-vpc",
				name:       "my-vpc",
				path:       "",
				modelParam: "qwen2:0.5b",
				pull:       true,
			},
		},
		"valid-pvc-withpath-and-modelname": {
			input: "pvc://my-vpc/path/to/model?model=qwen2:0.5b",
			want: modelURL{
				scheme:     "pvc",
				ref:        "my-vpc/path/to/model",
				name:       "my-vpc",
				path:       "path/to/model",
				modelParam: "qwen2:0.5b",
				pull:       true,
			},
		},
		"valid-ollama-with-no-pull": {
			input: "ollama://gemma2:2b?pull=false",
			want: modelURL{
				scheme: "ollama",
				ref:    "gemma2:2b",
				name:   "gemma2:2b",
				path:   "",
				pull:   false,
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := parseModelURL(c.input)
			if c.wantErr {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}
			c.want.original = c.input
			require.Equal(t, c.want, got)
		})
	}
}

func TestModelURLRejectsShellSyntax(t *testing.T) {
	t.Parallel()
	// Both the reference and the decoded PVC model parameter must keep #656's
	// validation boundary. Encoded shell syntax is not a safe model name.
	for _, value := range []string{
		"model;id", "model&&id", "model|id", "$(id)", "`id`",
		"model name", "model\tname", "model\nname", "model'name", `model"name`,
		"model>file", "model<file", "model\\name",
		"model%3Bid", "%24%28id%29", "%60id%60", "model%20name", "model%0Aname",
	} {
		for _, prefix := range []string{"ollama://", "pvc://models?model="} {
			t.Run(prefix+value, func(t *testing.T) {
				t.Parallel()
				input := value
				if prefix == "pvc://models?model=" {
					// Raw & separates query parameters; encode it to test an
					// ampersand in the model value itself.
					input = strings.ReplaceAll(input, "&", "%26")
				}
				_, err := parseModelURL(prefix + input)
				require.Error(t, err)
			})
		}
	}
}
