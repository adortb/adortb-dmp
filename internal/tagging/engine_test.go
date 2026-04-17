package tagging

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/adortb/adortb-dmp/internal/behavior"
)

// mockTagStore 用于测试的内存 TagStore
type mockTagStore struct {
	mu     sync.Mutex
	scores map[string]map[string]float32 // userID -> tagID -> score
	counts map[string]int                // key -> count
}

func newMockStore() *mockTagStore {
	return &mockTagStore{
		scores: make(map[string]map[string]float32),
		counts: make(map[string]int),
	}
}

func (m *mockTagStore) IncrTagScore(_ context.Context, userID, tagID string, delta float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.scores[userID] == nil {
		m.scores[userID] = make(map[string]float32)
	}
	m.scores[userID][tagID] += delta
	return nil
}

func (m *mockTagStore) GetEventCount(_ context.Context, userID string, cond Condition) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := userID + ":" + string(cond.EventType) + ":" + cond.Category
	return m.counts[key], nil
}

func (m *mockTagStore) GetTotalEventCount(_ context.Context, userID string, cond Condition) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[userID+":total"], nil
}

func (m *mockTagStore) setCount(userID string, eventType behavior.EventType, category string, count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[userID+":"+string(eventType)+":"+category] = count
}

func (m *mockTagStore) setTotalCount(userID string, count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[userID+":total"] = count
}

func (m *mockTagStore) getScore(userID, tagID string) float32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.scores[userID][tagID]
}

func TestEngineApply_HighValueUser(t *testing.T) {
	store := newMockStore()
	rule := Rule{
		ID:    "test:high_value",
		TagID: "high_value_user",
		Conditions: []Condition{
			{EventType: behavior.EventPurchase, MinValue: 100},
		},
		Weight: 1.0,
	}
	engine := NewEngine(store, []Rule{rule})

	event := &behavior.Event{
		UserID:    "u1",
		EventType: behavior.EventPurchase,
		Value:     150.0,
		Timestamp: time.Now(),
	}

	if err := engine.Apply(context.Background(), event); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	score := store.getScore("u1", "high_value_user")
	if score != 1.0 {
		t.Errorf("expected score 1.0, got %f", score)
	}
}

func TestEngineApply_HighValueUser_NotMet(t *testing.T) {
	store := newMockStore()
	rule := Rule{
		ID:    "test:high_value",
		TagID: "high_value_user",
		Conditions: []Condition{
			{EventType: behavior.EventPurchase, MinValue: 100},
		},
		Weight: 1.0,
	}
	engine := NewEngine(store, []Rule{rule})

	event := &behavior.Event{
		UserID:    "u2",
		EventType: behavior.EventPurchase,
		Value:     50.0, // 低于阈值
		Timestamp: time.Now(),
	}

	if err := engine.Apply(context.Background(), event); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	score := store.getScore("u2", "high_value_user")
	if score != 0 {
		t.Errorf("expected score 0, got %f", score)
	}
}

func TestEngineApply_InterestTag_CountMet(t *testing.T) {
	st := newMockStore()
	st.setCount("u3", behavior.EventView, "electronics", 5)

	rule := Rule{
		ID:    "test:interest:electronics",
		TagID: "interest:electronics",
		Conditions: []Condition{
			{EventType: behavior.EventView, Category: "electronics", MinCount: 5, TimeWindow: 7 * 24 * time.Hour},
		},
		Weight: 0.8,
	}
	engine := NewEngine(st, []Rule{rule})

	event := &behavior.Event{
		UserID:    "u3",
		EventType: behavior.EventView,
		Category:  "electronics",
		Timestamp: time.Now(),
	}

	if err := engine.Apply(context.Background(), event); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	score := st.getScore("u3", "interest:electronics")
	if score != 0.8 {
		t.Errorf("expected score 0.8, got %f", score)
	}
}

func TestEngineApply_InterestTag_CountNotMet(t *testing.T) {
	st := newMockStore()
	st.setCount("u4", behavior.EventView, "electronics", 3) // 不足 5 次

	rule := Rule{
		ID:    "test:interest:electronics",
		TagID: "interest:electronics",
		Conditions: []Condition{
			{EventType: behavior.EventView, Category: "electronics", MinCount: 5, TimeWindow: 7 * 24 * time.Hour},
		},
		Weight: 0.8,
	}
	engine := NewEngine(st, []Rule{rule})

	event := &behavior.Event{
		UserID:    "u4",
		EventType: behavior.EventView,
		Category:  "electronics",
		Timestamp: time.Now(),
	}

	if err := engine.Apply(context.Background(), event); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	score := st.getScore("u4", "interest:electronics")
	if score != 0 {
		t.Errorf("expected score 0, got %f", score)
	}
}

func TestEngineApply_ActiveUser(t *testing.T) {
	st := newMockStore()
	st.setTotalCount("u5", 10) // 恰好满足

	rule := Rule{
		ID:    "test:active",
		TagID: "active_user",
		Conditions: []Condition{
			{MinCount: 10, TimeWindow: 7 * 24 * time.Hour},
		},
		Weight: 0.6,
	}
	engine := NewEngine(st, []Rule{rule})

	event := &behavior.Event{
		UserID:    "u5",
		EventType: behavior.EventView,
		Timestamp: time.Now(),
	}

	if err := engine.Apply(context.Background(), event); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	score := st.getScore("u5", "active_user")
	if score != 0.6 {
		t.Errorf("expected score 0.6, got %f", score)
	}
}

func TestEngineAddRule(t *testing.T) {
	engine := NewEngine(newMockStore(), nil)
	if len(engine.Rules()) != 0 {
		t.Error("expected empty rules")
	}

	engine.AddRule(Rule{ID: "r1", TagID: "tag1"})
	if len(engine.Rules()) != 1 {
		t.Error("expected 1 rule after AddRule")
	}
}

func TestEngineApply_WrongEventType(t *testing.T) {
	st := newMockStore()
	rule := Rule{
		ID:    "test:click_only",
		TagID: "click_user",
		Conditions: []Condition{
			{EventType: behavior.EventClick, MinCount: 1},
		},
		Weight: 0.5,
	}
	engine := NewEngine(st, []Rule{rule})

	// 发送 view 事件，不应命中 click 规则
	event := &behavior.Event{
		UserID:    "u6",
		EventType: behavior.EventView,
		Timestamp: time.Now(),
	}
	if err := engine.Apply(context.Background(), event); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	score := st.getScore("u6", "click_user")
	if score != 0 {
		t.Errorf("view event should not trigger click rule, got score %f", score)
	}
}

func TestDefaultRulesCount(t *testing.T) {
	if len(DefaultRules) != 10 {
		t.Errorf("expected 10 default rules, got %d", len(DefaultRules))
	}
}
