#!/bin/bash

source $REPO_DIR/test/e2e/common.sh

models_release="kubeai-models"
model_name="qwen-500m-cpu"

# Create PV_HOST_PATH inside the kind container
kind_container=$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[0].metadata.name}')
export PV_HOST_PATH="/mnt/models"
docker exec -i "$kind_container" mkdir -p "$PV_HOST_PATH"
echo "PV_HOST_PATH: $PV_HOST_PATH"


envsubst < $REPO_DIR/test/e2e/engine-ollama-pvc/pv.yaml | kubectl apply -f -
kubectl apply -f $REPO_DIR/test/e2e/engine-ollama-pvc/pvc.yaml

# Apply the Ollama hydrate job
kubectl apply -f $REPO_DIR/test/e2e/engine-ollama-pvc/ollama-hydrate-job.yaml

# Wait for job completion with timeout
echo "Waiting for Ollama hydrate job to complete..."
if ! kubectl wait --for=condition=complete --timeout=600s job/ollama-pvc-hydrate; then
    echo "Ollama hydrate job failed or timed out"
    kubectl logs job/ollama-pvc-hydrate
    exit 1
fi


helm install $models_release $REPO_DIR/charts/models -f - <<EOF
catalog:
  qwen-500m-cpu:
    enabled: true
    url: pvc://model-pvc?model=qwen:0.5b
    minReplicas: 2
    maxReplicas: 2
    engine: OLlama
    resourceProfile: "cpu:1" 
    features: [TextGeneration]
EOF

# Wait for both replicas, including their startup probes, to become Ready.
replicas_ready() {
  kubectl get pods -l "model=$model_name" -o json | jq -e '
    [.items[] | select(.metadata.deletionTimestamp == null)] as $pods |
    ($pods | length == 2) and
    all($pods[]; any(.status.conditions[]?; .type == "Ready" and .status == "True"))
  '
}
retry 600 replicas_ready

verify_completion() {
  local endpoint=$1
  local response_file=$2
  local http_status
  http_status=$(curl --fail --silent --show-error "$endpoint" \
    --max-time 600 \
    --output "$response_file" \
    --write-out '%{http_code}' \
    -H "Content-Type: application/json" \
    -d '{"model": "qwen-500m-cpu", "prompt": "Who was the first president of the United States?", "max_tokens": 40, "stream": false}')
  test "$http_status" = 200
  cat "$response_file"
  jq -e --arg model "$model_name" '
    .model == $model and
    (.choices | length > 0) and
    (.choices[0].text | type == "string" and test("\\S"))
  ' "$response_file"
}

verify_pod() (
  # A subshell keeps this port-forward cleanup separate from the test harness.
  local pod=$1
  local port_forward_log="$TMP_DIR/$pod-port-forward.log"
  local port
  kubectl port-forward --address 127.0.0.1 "pod/$pod" :8000 > "$port_forward_log" 2>&1 &
  local port_forward_pid=$!
  trap 'kill "$port_forward_pid" 2>/dev/null || true; wait "$port_forward_pid" 2>/dev/null || true' EXIT

  port_forward_started() {
    port=$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) -> 8000$/\1/p' "$port_forward_log")
    test -n "$port"
  }
  retry 60 port_forward_started

  echo "Verifying model alias and completion on $pod"
  curl --fail --silent --show-error --max-time 10 \
    "http://127.0.0.1:$port/api/tags" --output "$TMP_DIR/$pod-tags.json"
  jq -e --arg model "$model_name:latest" \
    'any(.models[]; .name == $model)' "$TMP_DIR/$pod-tags.json"
  # The startup probe must preload the alias before the first API request.
  curl --fail --silent --show-error --max-time 10 \
    "http://127.0.0.1:$port/api/ps" --output "$TMP_DIR/$pod-loaded.json"
  jq -e --arg model "$model_name:latest" \
    'any(.models[]; .name == $model)' "$TMP_DIR/$pod-loaded.json"
  verify_completion "http://127.0.0.1:$port/v1/completions" "$TMP_DIR/$pod-completion.json"
)

# Direct requests establish that each replica is usable independently.
pods=$(kubectl get pods -l "model=$model_name" -o json | jq -er '
  [.items[] | select(.metadata.deletionTimestamp == null)] |
  if length == 2 then .[].metadata.name else error("expected two active Ollama pods") end
')
for pod in $pods; do
  verify_pod "$pod"
done

# Check the routed KubeAI API separately from direct pod access.
verify_completion http://localhost:8000/openai/v1/completions "$TMP_DIR/ollama-routed-completion.json"
