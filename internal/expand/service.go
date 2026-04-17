package expand

import (
	"context"
	"fmt"
)

const defaultExpandLimit = 1000

// TagStore 扩展服务依赖的存储接口
type TagStore interface {
	GetTagUsers(ctx context.Context, tagID string, limit int64) ([]string, error)
}

// Request 受众扩展请求
type Request struct {
	SeedTags   []string `json:"seed_tags"`
	MinOverlap int      `json:"min_overlap"`
	Limit      int      `json:"limit"`
}

// Result 扩展结果
type Result struct {
	UserIDs []string `json:"user_ids"`
	Total   int      `json:"total"`
}

// Service 相似受众扩展服务（基于标签交集）
type Service struct {
	store TagStore
}

func NewService(store TagStore) *Service {
	return &Service{store: store}
}

// Expand 按标签交集扩展相似受众
func (s *Service) Expand(ctx context.Context, req *Request) (*Result, error) {
	if len(req.SeedTags) == 0 {
		return &Result{}, nil
	}
	if req.MinOverlap <= 0 {
		req.MinOverlap = 1
	}
	if req.Limit <= 0 || req.Limit > 10000 {
		req.Limit = defaultExpandLimit
	}

	hitCount := make(map[string]int, 10000)

	for _, tagID := range req.SeedTags {
		users, err := s.store.GetTagUsers(ctx, tagID, int64(req.Limit*len(req.SeedTags)))
		if err != nil {
			return nil, fmt.Errorf("get tag users %s: %w", tagID, err)
		}
		for _, uid := range users {
			hitCount[uid]++
		}
	}

	type scored struct {
		uid   string
		count int
	}
	candidates := make([]scored, 0, len(hitCount))
	for uid, cnt := range hitCount {
		if cnt >= req.MinOverlap {
			candidates = append(candidates, scored{uid: uid, count: cnt})
		}
	}

	// 插入排序（按 count 倒序）
	for i := 1; i < len(candidates); i++ {
		key := candidates[i]
		j := i - 1
		for j >= 0 && candidates[j].count < key.count {
			candidates[j+1] = candidates[j]
			j--
		}
		candidates[j+1] = key
	}

	limit := req.Limit
	if limit > len(candidates) {
		limit = len(candidates)
	}
	userIDs := make([]string, limit)
	for i := 0; i < limit; i++ {
		userIDs[i] = candidates[i].uid
	}

	return &Result{
		UserIDs: userIDs,
		Total:   len(candidates),
	}, nil
}
