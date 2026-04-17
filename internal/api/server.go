package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/adortb/adortb-dmp/internal/behavior"
	"github.com/adortb/adortb-dmp/internal/expand"
	"github.com/adortb/adortb-dmp/internal/metrics"
	"github.com/adortb/adortb-dmp/internal/store"
)

// BehaviorProcessor 行为事件处理接口
type BehaviorProcessor interface {
	Process(ctx context.Context, event *behavior.Event) error
}

// LookupService 标签查询接口
type LookupService interface {
	GetUserTags(ctx context.Context, userID string) (*store.UserTags, error)
	GetUserProfile(ctx context.Context, userID string) (*store.UserProfile, error)
	BatchGetUserTags(ctx context.Context, userIDs []string) (map[string]*store.UserTags, error)
	GetTagUsers(ctx context.Context, tagID string, limit int64) ([]string, error)
}

// ExpandService 受众扩展接口
type ExpandService interface {
	Expand(ctx context.Context, req *expand.Request) (*expand.Result, error)
}

// Server DMP HTTP API 服务
type Server struct {
	processor BehaviorProcessor
	lookup    LookupService
	expand    ExpandService
	logger    *slog.Logger
	counters  *metrics.Counters
	mux       *http.ServeMux
}

func NewServer(
	processor BehaviorProcessor,
	lookupSvc LookupService,
	expandSvc ExpandService,
	logger *slog.Logger,
	counters *metrics.Counters,
) *Server {
	s := &Server{
		processor: processor,
		lookup:    lookupSvc,
		expand:    expandSvc,
		logger:    logger,
		counters:  counters,
		mux:       http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/v1/behavior", s.handleBehavior)
	s.mux.HandleFunc("/v1/user/", s.handleUser)
	s.mux.HandleFunc("/v1/tags/batch", s.handleBatchTags)
	s.mux.HandleFunc("/v1/audience/expand", s.handleExpand)
	s.mux.HandleFunc("/v1/audience/", s.handleAudience)
	s.mux.HandleFunc("/metrics", metrics.Handler(s.counters))
	s.mux.HandleFunc("/health", s.handleHealth)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// POST /v1/behavior
func (s *Server) handleBehavior(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.counters.BehaviorReceived.Add(1)

	var event behavior.Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if err := event.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.processor.Process(r.Context(), &event); err != nil {
		s.counters.BehaviorFailed.Add(1)
		s.logger.Error("process behavior failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.counters.BehaviorProcessed.Add(1)
	w.WriteHeader(http.StatusAccepted)
}

// GET /v1/user/:user_id/tags
// GET /v1/user/:user_id/profile
func (s *Server) handleUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/user/"), "/")
	if len(parts) < 2 {
		writeError(w, http.StatusBadRequest, "invalid path")
		return
	}
	userID := parts[0]
	action := parts[1]

	s.counters.LookupRequests.Add(1)

	switch action {
	case "tags":
		tags, err := s.lookup.GetUserTags(r.Context(), userID)
		if err != nil {
			s.logger.Error("get user tags failed", "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		writeJSON(w, tags)

	case "profile":
		profile, err := s.lookup.GetUserProfile(r.Context(), userID)
		if err != nil {
			s.logger.Error("get user profile failed", "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		writeJSON(w, profile)

	default:
		writeError(w, http.StatusNotFound, "unknown action: "+action)
	}
}

// POST /v1/tags/batch
func (s *Server) handleBatchTags(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.counters.LookupRequests.Add(1)

	var req struct {
		UserIDs []string `json:"user_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if len(req.UserIDs) == 0 {
		writeJSON(w, map[string]interface{}{})
		return
	}
	if len(req.UserIDs) > 200 {
		writeError(w, http.StatusBadRequest, "too many user_ids (max 200)")
		return
	}

	result, err := s.lookup.BatchGetUserTags(r.Context(), req.UserIDs)
	if err != nil {
		s.logger.Error("batch get tags failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, result)
}

// GET /v1/audience/:tag_id/users?limit=1000
func (s *Server) handleAudience(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/audience/"), "/")
	if len(parts) < 2 || parts[1] != "users" {
		writeError(w, http.StatusBadRequest, "invalid path, expect /v1/audience/:tag_id/users")
		return
	}
	tagID := parts[0]

	limit := int64(1000)
	if v := r.URL.Query().Get("limit"); v != "" {
		var n int64
		if ok := parseInt64(v, &n); ok && n > 0 && n <= 10000 {
			limit = n
		}
	}

	users, err := s.lookup.GetTagUsers(r.Context(), tagID, limit)
	if err != nil {
		s.logger.Error("get tag users failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, map[string]interface{}{
		"tag_id": tagID,
		"users":  users,
		"total":  len(users),
	})
}

// POST /v1/audience/expand
func (s *Server) handleExpand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req expand.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}

	result, err := s.expand.Expand(r.Context(), &req)
	if err != nil {
		s.logger.Error("audience expand failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, result)
}

// GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]interface{}{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "encode response failed", http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func parseInt64(s string, out *int64) bool {
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
		n = n*10 + int64(c-'0')
	}
	*out = n
	return true
}
