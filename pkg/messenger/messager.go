package messenger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/awssnssqs"
	_ "gocloud.dev/pubsub/azuresb"
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/kafkapubsub"
	_ "gocloud.dev/pubsub/natspubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"
)

type Messenger struct {
	Deployments DeploymentManager
	Endpoints   EndpointManager
	Queues      QueueManager

	HTTPC *http.Client

	MaxHandlers int

	requests  *pubsub.Subscription
	responses *pubsub.Topic
}

func NewMessenger(
	ctx context.Context,
	requestsURL string,
	responsesURL string,
	maxHandlers int,
	deployments DeploymentManager,
	endpoints EndpointManager,
	queues QueueManager,
	httpClient *http.Client,
) (*Messenger, error) {
	requests, err := pubsub.OpenSubscription(ctx, requestsURL)
	if err != nil {
		return nil, err
	}

	responses, err := pubsub.OpenTopic(ctx, responsesURL)
	if err != nil {
		return nil, err
	}

	return &Messenger{
		Deployments: deployments,
		Endpoints:   endpoints,
		Queues:      queues,
		HTTPC:       httpClient,
		requests:    requests,
		responses:   responses,
		MaxHandlers: maxHandlers,
	}, nil
}

func (m *Messenger) Start(ctx context.Context) error {
	sem := make(chan struct{}, m.MaxHandlers)

recvLoop:
	for {
		msg, err := m.requests.Receive(ctx)
		if err != nil {
			return err
		}

		log.Println("Received message:", msg.LoggableID)

		// Wait if there are too many active handle goroutines and acquire the
		// semaphore. If the context is canceled, stop waiting and start shutting
		// down.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break recvLoop
		}

		go func() {
			defer func() { <-sem }()
			m.handleRequest(context.Background(), msg)
		}()
	}

	// We're no longer receiving messages. Wait to finish handling any
	// unacknowledged messages by totally acquiring the semaphore.
	for n := 0; n < m.MaxHandlers; n++ {
		sem <- struct{}{}
	}

	return nil
}

func (m *Messenger) handleRequest(ctx context.Context, msg *pubsub.Message) {
	// Expecting a message with the following structure:
	/*
		{
			"metadata": {
				"some-sort-of-id": 123,
				"optional-key": "some-user-value"
				# ...
			},
			"path": "/v1/completions",
			"body": {
				"model": "test-model"
				# ... other OpenAI compatible fields
			}
		}
	*/
	req, err := parseRequest(ctx, msg)
	if err != nil {
		m.sendResponse(req, m.jsonError("error parsing request: %v", err), http.StatusBadRequest)
		return
	}

	backendDeployment, backendExists := m.Deployments.ResolveDeployment(req.model)
	if !backendExists {
		// Send a 400 response to the client, however it is possible the backend
		// will be deployed soon or another subscriber will handle it.
		m.sendResponse(req, m.jsonError("backend not found for model: %v", req.model), http.StatusNotFound)
		return
	}

	// Ensure the backend is scaled to at least one Pod.
	m.Deployments.AtLeastOne(backendDeployment)

	log.Printf("Entering queue: %s", msg.LoggableID)

	complete := m.Queues.EnqueueAndWait(ctx, backendDeployment, msg.LoggableID)
	defer complete()

	log.Printf("Awaiting host for message %s", msg.LoggableID)

	host, err := m.Endpoints.AwaitHostAddress(ctx, backendDeployment, "http")
	if err != nil {
		m.sendResponse(req, m.jsonError("error awaiting host for backend: %v", err), http.StatusBadGateway)
		return
	}

	url := fmt.Sprintf("http://%s%s", host, req.path)
	log.Printf("Sending request to backend for message %s: %s", msg.LoggableID, url)
	respPayload, respCode, err := m.sendBackendRequest(ctx, url, req.body)
	if err != nil {
		m.sendResponse(req, m.jsonError("error sending request to backend: %v", err), http.StatusBadGateway)
	}

	m.sendResponse(req, respPayload, respCode)
}

func (m *Messenger) Stop(ctx context.Context) error {
	return m.requests.Shutdown(ctx)
}

type request struct {
	ctx      context.Context
	msg      *pubsub.Message
	metadata map[string]interface{}
	path     string
	body     json.RawMessage
	model    string
}

func parseRequest(ctx context.Context, msg *pubsub.Message) (*request, error) {
	req := &request{
		ctx: ctx,
		msg: msg,
	}

	var payload struct {
		Metadata map[string]interface{} `json:"metadata"`
		Path     string                 `json:"path"`
		Body     json.RawMessage        `json:"body"`
	}
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		return req, fmt.Errorf("unmarshalling message as json: %w", err)
	}

	path := payload.Path
	if payload.Path == "" {
		// Default to completions endpoint.
		path = "/v1/completions"
	} else if !strings.HasPrefix(payload.Path, "/") {
		path = "/" + payload.Path
	}

	req.metadata = payload.Metadata
	req.path = path
	req.body = payload.Body

	var payloadBody struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(payload.Body, &payloadBody); err != nil {
		return req, fmt.Errorf("unmarshalling message .body as json: %w", err)
	}

	if payloadBody.Model == "" {
		return req, fmt.Errorf("empty .body.model in message")
	}

	req.model = payloadBody.Model

	return req, nil
}

func (m *Messenger) sendBackendRequest(ctx context.Context, url string, body []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := m.HTTPC.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return payload, resp.StatusCode, nil
}

func (m *Messenger) sendResponse(req *request, body []byte, statusCode int) {
	log.Printf("Sending response to message: %v", req.msg.LoggableID)

	response := struct {
		Metadata   map[string]interface{} `json:"metadata"`
		StatusCode int                    `json:"status_code"`
		Body       json.RawMessage        `json:"body"`
	}{
		Metadata:   req.metadata,
		StatusCode: statusCode,
		Body:       body,
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Println("Error marshalling response:", err)
	}

	if err := m.responses.Send(req.ctx, &pubsub.Message{
		Body: jsonResponse,
		Metadata: map[string]string{
			"request_message_id": req.msg.LoggableID,
		},
	}); err != nil {
		log.Printf("Error sending response for message %s: %v", req.msg.LoggableID, err)

		// If a repsonse cant be sent, the message should be redelivered.
		if req.msg.Nackable() {
			req.msg.Nack()
		}
		return
	}

	log.Printf("Send response for message: %s", req.msg.LoggableID)
	req.msg.Ack()
}

func (m *Messenger) jsonError(format string, args ...interface{}) []byte {

	message := fmt.Sprintf(format, args...)
	log.Println(message)

	// Example OpenAI error response:
	/*
		{
		  "error": {
		    "message": "Invalid authorization header",
		    "type": "server_error",
		    "param": null,
		    "code": null
		  }
	*/
	return []byte(fmt.Sprintf(`{
	"error": {
		"message": %q
	}
}`, message))
}
