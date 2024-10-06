#!/bin/bash

source $REPO_ROOT/test/e2e/common.sh

model=faster-whisper-medium-en-cpu
kubectl apply -f $REPO_ROOT/manifests/models/$model.yaml

audio_file=$REPO_ROOT/tmp/kubeai.mp4
curl -L -o $audio_file https://github.com/user-attachments/assets/711d1279-6af9-4c6c-a052-e59e7730b757

transcription_file=$REPO_ROOT/tmp/transcription.json
retry 3 curl http://localhost:8000/openai/v1/audio/transcriptions \
  -F "file=@$audio_file" \
  -F "language=en" \
  -F "model=$model" > $transcription_file
  
result_contains_kubernetes=$(cat $transcription_file | jq '.text | ascii_downcase | contains("kubernetes")')
if [ "$result_contains_kubernetes" = "true" ]; then
  echo "The transcript contains 'kubernetes'."
else
  echo "The text does not contain 'kubernetes':"
  echo $result
  exit 1
fi
