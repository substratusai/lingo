package stats

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/substratusai/lingo/pkg/queue"
)

type Handler struct {
	Queues *queue.Manager
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if err := json.NewEncoder(w).Encode(Stats{
		ActiveRequests: h.Queues.TotalCounts(),
	}); err != nil {
		log.Printf("error writing response body: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
