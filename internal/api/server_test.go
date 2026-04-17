package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/adortb/adortb-dmp/internal/behavior"
	"github.com/adortb/adortb-dmp/internal/expand"
	"github.com/adortb/adortb-dmp/internal/metrics"
	"github.com/adortb/adortb-dmp/internal/store"
)

// ---- mock 实现 ----

type mockProcessor struct {
	lastEvent *behavior.Event
	failWith  error
}

func (m *mockProcessor) Process(_ context.Context, event *behavior.Event) error {
	if m.failWith != nil {
		return m.failWith
	}
	m.lastEvent = event
	return nil
}

type mockLookup struct{}

func (m *mockLookup) GetUserTags(_ context.Context, userID string) (*store.UserTags, error) {
	return &store.UserTags{
		UserID: userID,
		Tags:   map[string]float32{"interest:electronics": 0.8},
	}, nil
}

func (m *mockLookup) GetUserProfile(_ context.Context, userID string) (*store.UserProfile, error) {
	return &store.UserProfile{UserID: userID, Device: "ios"}, nil
}

func (m *mockLookup) BatchGetUserTags(_ context.Context, userIDs []string) (map[string]*store.UserTags, error) {
	out := make(map[string]*store.UserTags, len(userIDs))
	for _, uid := range userIDs {
		out[uid] = &store.UserTags{UserID: uid, Tags: map[string]float32{"tag:a": 1.0}}
	}
	return out, nil
}

func (m *mockLookup) GetTagUsers(_ context.Context, _ string, _ int64) ([]string, error) {
	return []string{"u1", "u2"}, nil
}

type mockExpand struct{}

func (m *mockExpand) Expand(_ context.Context, _ *expand.Request) (*expand.Result, error) {
	return &expand.Result{UserIDs: []string{"u1"}, Total: 1}, nil
}

func buildServer(proc *mockProcessor) *Server {
	return NewServer(proc, &mockLookup{}, &mockExpand{}, slog.Default(), &metrics.Counters{})
}

// ---- 测试用例 ----

func TestHandleBehavior_Valid(t *testing.T) {
	proc := &mockProcessor{}
	s := buildServer(proc)

	body := `{"user_id":"u1","event_type":"view","item_id":"i1","category":"electronics","value":9.9}`
	req := httptest.NewRequest(http.MethodPost, "/v1/behavior", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	s.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	if proc.lastEvent == nil {
		t.Error("processor should have been called")
	}
	if proc.lastEvent.UserID != "u1" {
		t.Errorf("expected userID u1, got %s", proc.lastEvent.UserID)
	}
}

func TestHandleBehavior_InvalidJSON(t *testing.T) {
	s := buildServer(&mockProcessor{})
	req := httptest.NewRequest(http.MethodPost, "/v1/behavior", bytes.NewBufferString("not-json"))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleBehavior_MissingUserID(t *testing.T) {
	s := buildServer(&mockProcessor{})
	body := `{"event_type":"view"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/behavior", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleBehavior_WrongMethod(t *testing.T) {
	s := buildServer(&mockProcessor{})
	req := httptest.NewRequest(http.MethodGet, "/v1/behavior", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleUserTags(t *testing.T) {
	s := buildServer(&mockProcessor{})
	req := httptest.NewRequest(http.MethodGet, "/v1/user/u1/tags", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp store.UserTags
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.UserID != "u1" {
		t.Errorf("expected user_id u1, got %s", resp.UserID)
	}
}

func TestHandleUserProfile(t *testing.T) {
	s := buildServer(&mockProcessor{})
	req := httptest.NewRequest(http.MethodGet, "/v1/user/u1/profile", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHandleBatchTags_Valid(t *testing.T) {
	s := buildServer(&mockProcessor{})
	body := `{"user_ids":["u1","u2"]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tags/batch", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleBatchTags_TooMany(t *testing.T) {
	s := buildServer(&mockProcessor{})
	ids := make([]string, 201)
	for i := range ids {
		ids[i] = "u1"
	}
	body, _ := json.Marshal(map[string]interface{}{"user_ids": ids})
	req := httptest.NewRequest(http.MethodPost, "/v1/tags/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleExpand(t *testing.T) {
	s := buildServer(&mockProcessor{})
	body := `{"seed_tags":["tag:a"],"min_overlap":1,"limit":10}`
	req := httptest.NewRequest(http.MethodPost, "/v1/audience/expand", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleAudience_Users(t *testing.T) {
	s := buildServer(&mockProcessor{})
	req := httptest.NewRequest(http.MethodGet, "/v1/audience/interest:electronics/users?limit=10", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleHealth(t *testing.T) {
	s := buildServer(&mockProcessor{})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status=ok, got %v", resp["status"])
	}
}

func TestHandleMetrics(t *testing.T) {
	s := buildServer(&mockProcessor{})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestCounters_BehaviorTracked(t *testing.T) {
	proc := &mockProcessor{}
	counters := &metrics.Counters{}
	s := NewServer(proc, &mockLookup{}, &mockExpand{}, slog.Default(), counters)

	body := `{"user_id":"u1","event_type":"click"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/behavior", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if counters.BehaviorReceived.Load() != 1 {
		t.Errorf("expected BehaviorReceived=1, got %d", counters.BehaviorReceived.Load())
	}
	if counters.BehaviorProcessed.Load() != 1 {
		t.Errorf("expected BehaviorProcessed=1, got %d", counters.BehaviorProcessed.Load())
	}
}
