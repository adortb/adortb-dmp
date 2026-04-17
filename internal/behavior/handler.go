package behavior

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
)

// Processor 处理行为事件的接口
type Processor interface {
	Process(ctx context.Context, event *Event) error
}

// Handler HTTP 行为事件处理器
type Handler struct {
	processor Processor
	logger    *slog.Logger
}

func NewHandler(processor Processor, logger *slog.Logger) *Handler {
	return &Handler{processor: processor, logger: logger}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err := event.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.processor.Process(r.Context(), &event); err != nil {
		h.logger.Error("process behavior failed", "error", err, "user_id", event.UserID)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
