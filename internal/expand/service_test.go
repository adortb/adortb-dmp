package expand

import (
	"context"
	"testing"
)

// mockTagStore 测试用内存存储
type mockTagStore struct {
	tagUsers map[string][]string
}

func newMock() *mockTagStore {
	return &mockTagStore{tagUsers: make(map[string][]string)}
}

func (m *mockTagStore) set(tagID string, users []string) {
	m.tagUsers[tagID] = users
}

func (m *mockTagStore) GetTagUsers(_ context.Context, tagID string, limit int64) ([]string, error) {
	users := m.tagUsers[tagID]
	if int64(len(users)) > limit {
		return users[:limit], nil
	}
	return users, nil
}

func newTestService(m *mockTagStore) *Service {
	return NewService(m)
}

func TestExpand_BasicOverlap(t *testing.T) {
	m := newMock()
	m.set("tag:a", []string{"u1", "u2", "u3"})
	m.set("tag:b", []string{"u1", "u3", "u4"})

	svc := newTestService(m)
	result, err := svc.Expand(context.Background(), &Request{
		SeedTags:   []string{"tag:a", "tag:b"},
		MinOverlap: 2,
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("Expand failed: %v", err)
	}

	if result.Total != 2 {
		t.Errorf("expected 2 users with overlap>=2, got %d", result.Total)
	}
	found := make(map[string]bool)
	for _, uid := range result.UserIDs {
		found[uid] = true
	}
	if !found["u1"] || !found["u3"] {
		t.Errorf("expected u1 and u3 in result, got %v", result.UserIDs)
	}
}

func TestExpand_MinOverlap1(t *testing.T) {
	m := newMock()
	m.set("tag:x", []string{"u1", "u2"})
	m.set("tag:y", []string{"u3"})

	svc := newTestService(m)
	result, err := svc.Expand(context.Background(), &Request{
		SeedTags:   []string{"tag:x", "tag:y"},
		MinOverlap: 1,
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("Expand failed: %v", err)
	}

	if result.Total != 3 {
		t.Errorf("expected 3 users with overlap>=1, got %d", result.Total)
	}
}

func TestExpand_EmptySeedTags(t *testing.T) {
	svc := newTestService(newMock())
	result, err := svc.Expand(context.Background(), &Request{SeedTags: nil})
	if err != nil {
		t.Fatalf("Expand failed: %v", err)
	}
	if result.Total != 0 {
		t.Errorf("expected 0 results for empty seed tags")
	}
}

func TestExpand_LimitRespected(t *testing.T) {
	m := newMock()
	users := make([]string, 50)
	for i := range users {
		// 生成不重复的用户 ID
		users[i] = "user_" + string(rune('a'+i%26)) + string(rune('0'+i/26))
	}
	m.set("tag:big", users)

	svc := newTestService(m)
	result, err := svc.Expand(context.Background(), &Request{
		SeedTags:   []string{"tag:big"},
		MinOverlap: 1,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("Expand failed: %v", err)
	}

	if len(result.UserIDs) > 10 {
		t.Errorf("expected at most 10 users, got %d", len(result.UserIDs))
	}
}

func TestExpand_SortByHitCount(t *testing.T) {
	m := newMock()
	m.set("tag:1", []string{"u1", "u2", "u3"})
	m.set("tag:2", []string{"u1", "u2"})
	m.set("tag:3", []string{"u1"})

	svc := newTestService(m)
	result, err := svc.Expand(context.Background(), &Request{
		SeedTags:   []string{"tag:1", "tag:2", "tag:3"},
		MinOverlap: 1,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("Expand failed: %v", err)
	}

	if len(result.UserIDs) == 0 {
		t.Fatal("expected results")
	}
	if result.UserIDs[0] != "u1" {
		t.Errorf("expected u1 first (highest overlap), got %s", result.UserIDs[0])
	}
}

func TestExpand_DefaultLimit(t *testing.T) {
	m := newMock()
	users := make([]string, 100)
	for i := range users {
		users[i] = "u" + string(rune('A'+i%26)) + string(rune('0'+i/26))
	}
	m.set("tag:x", users)

	svc := newTestService(m)
	result, err := svc.Expand(context.Background(), &Request{
		SeedTags:   []string{"tag:x"},
		MinOverlap: 1,
		Limit:      0, // 触发默认值
	})
	if err != nil {
		t.Fatalf("Expand failed: %v", err)
	}
	if len(result.UserIDs) > defaultExpandLimit {
		t.Errorf("should not exceed default limit %d", defaultExpandLimit)
	}
}
