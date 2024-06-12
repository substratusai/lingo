package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gocloud.dev/pubsub"
)

func TestMessenger(t *testing.T) {

	const modelName = "test-model-a-for-messenger"
	deploy := testDeployment(modelName)

	require.NoError(t, testK8sClient.Create(testCtx, deploy))

	backendComplete := make(chan struct{})
	testBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Serving request from testBackend")

		var reqBody struct {
			Model string `json:"model"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&reqBody))
		require.Equal(t, modelName, reqBody.Model)

		<-backendComplete

		w.Write([]byte(fmt.Sprintf(`{"model": %q, "choices": [{"text": "hey"}]}`, reqBody.Model)))
	}))
	t.Logf("testBackend URL: %s", testBackend.URL)

	mockEndpointSlice(t, modelName, testBackend)

	// Wait for deployment mapping to sync.
	time.Sleep(3 * time.Second)

	// Send request id "a"
	sendRequestMessage(t, "/v1/completions", modelName, "a")

	// Assert on replicas before completing the request - otherwise there is a race condition
	// with the autoscaler.
	requireDeploymentReplicas(t, deploy, 1)
	completeRequests(backendComplete, 1)

	shouldReceiveResponseMessage(t, modelName, "a")

	sendRequestMessage(t, "/v1/completions", "non-existant-model", "b")
	shouldReceiveResponseErrMessage(t, http.StatusNotFound, "backend not found for model: non-existant-model", "b")
}

func shouldReceiveResponseErrMessage(t *testing.T, statusCode int, message string, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Logf("Waiting for response error message: id %q", id)
	resp, err := testResponsesSubscription.Receive(ctx)
	require.NoError(t, err)
	resp.Ack()

	require.JSONEq(t, fmt.Sprintf(`
{
	"metadata": {"my-id": %q},
	"status_code": %d,
	"body": {
		"error": {
			"message": %q
		}
	}
}`, id, statusCode, message), string(resp.Body))
}

func shouldReceiveResponseMessage(t *testing.T, model, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Logf("Waiting for response message for model %q and id %q", model, id)
	resp, err := testResponsesSubscription.Receive(ctx)
	require.NoError(t, err)
	resp.Ack()
	require.JSONEq(t, fmt.Sprintf(`
{
	"metadata": {"my-id": %q},
	"status_code": 200,
	"body": {
		"model": %q,
		"choices": [
			{"text": "hey"}
		]
	}
}`, id, model), string(resp.Body))
	require.Equal(t, "test-value", resp.Metadata["test-meta"])
}

func sendRequestMessage(t *testing.T, path, modelName, id string) {
	body := []byte(fmt.Sprintf(`
{
	"path": %q,
	"body": {
		"model": %q
	},
	"metadata": {"my-id": %q}
}`,
		path, modelName, id))

	err := testRequestsTopic.Send(context.Background(), &pubsub.Message{
		Body: body,
	})
	require.NoError(t, err)
}
