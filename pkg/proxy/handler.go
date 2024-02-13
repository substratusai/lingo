package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/substratusai/lingo/pkg/deployments"
	"github.com/substratusai/lingo/pkg/endpoints"
	"github.com/substratusai/lingo/pkg/queue"
)

// Handler serves http requests for end-clients.
// It is also responsible for triggering scale-from-zero.
type Handler struct {
	Deployments *deployments.Manager
	Endpoints   *endpoints.Manager
	Queues      *queue.Manager
}

func NewHandler(deployments *deployments.Manager, endpoints *endpoints.Manager, queues *queue.Manager) *Handler {
	return &Handler{Deployments: deployments, Endpoints: endpoints, Queues: queues}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var modelName string
	captureStatusRespWriter := newCaptureStatusCodeResponseWriter(w)
	w = captureStatusRespWriter
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		httpDuration.WithLabelValues(modelName, strconv.Itoa(captureStatusRespWriter.statusCode)).Observe(v)
	}))
	defer timer.ObserveDuration()

	id := uuid.New().String()
	log.Printf("request: %v", r.URL)
	w.Header().Set("X-Proxy", "lingo")

	var (
		proxyRequest *http.Request
		err          error
	)
	// TODO: Only parse model for paths that would have a model.
	modelName, proxyRequest, err = parseModel(r)
	if err != nil || modelName == "" {
		modelName = "unknown"
		log.Printf("error reading model from request body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("Bad request: unable to parse .model from JSON payload"))
		return
	}
	log.Println("model:", modelName)

	deploy, found := h.Deployments.ResolveDeployment(modelName)
	if !found {
		log.Printf("deployment not found for model: %v", err)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(fmt.Sprintf("Deployment for model not found: %v", modelName)))
		return
	}

	h.Deployments.AtLeastOne(deploy)

	log.Println("Entering queue", id)
	complete := h.Queues.EnqueueAndWait(r.Context(), deploy, id)
	log.Println("Admitted into queue", id)
	defer complete()

	// abort when deployment was removed meanwhile
	if _, exists := h.Deployments.ResolveDeployment(modelName); !exists {
		log.Printf("deployment not active for model removed: %v", err)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(fmt.Sprintf("Deployment for model not found: %v", modelName)))
		return
	}

	log.Println("Waiting for IPs", id)
	host, err := h.Endpoints.AwaitHostAddress(r.Context(), deploy, "http")
	if err != nil {
		log.Printf("error while finding the host address %v", err)
		switch {
		case errors.Is(err, context.Canceled):
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Request canceled"))
			return
		case errors.Is(err, context.DeadlineExceeded):
			w.WriteHeader(http.StatusGatewayTimeout)
			_, _ = w.Write([]byte(fmt.Sprintf("Request timed out for model: %v", modelName)))
			return
		default:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Internal server error"))
			return
		}
	}
	log.Printf("Got host: %v, id: %v\n", host, id)

	// TODO: Avoid creating new reverse proxies for each request.
	// TODO: Consider implementing a round robin scheme.
	log.Printf("Proxying request to host %v: %v\n", host, id)
	newReverseProxy(host).ServeHTTP(w, proxyRequest)
}

// parseModel parses the model name from the request
// returns empty string when none found or an error for failures on the proxy request object.
func parseModel(r *http.Request) (string, *http.Request, error) {
	if model := r.Header.Get("X-Model"); model != "" {
		return model, r, nil
	}
	// parse request body for model name, ignore errors
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", r, nil
	}

	var payload struct {
		Model string `json:"model"`
	}
	var model string
	if err := json.Unmarshal(body, &payload); err == nil {
		model = payload.Model
	}

	// create new request object
	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), bytes.NewReader(body))
	if err != nil {
		return "", nil, fmt.Errorf("create proxy request: %w", err)
	}
	proxyReq.Header = r.Header
	if err := proxyReq.ParseForm(); err != nil {
		return "", nil, fmt.Errorf("parse proxy form: %w", err)
	}
	return model, proxyReq, nil
}

// AdditionalProxyRewrite is an injection point for modifying proxy requests.
// Used in tests.
var AdditionalProxyRewrite = func(*httputil.ProxyRequest) {}

func newReverseProxy(host string) *httputil.ReverseProxy {
	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(&url.URL{
				Scheme: "http",
				Host:   host,
			})
			r.Out.Host = r.In.Host
			AdditionalProxyRewrite(r)
		},
	}
	return proxy
}
