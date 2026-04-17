package lookup

import (
	"context"
	"fmt"

	"github.com/adortb/adortb-dmp/internal/store"
)

// redisStore 依赖的 Redis 操作接口
type redisStore interface {
	GetUserTags(ctx context.Context, userID string) (*store.UserTags, error)
	GetUserProfile(ctx context.Context, userID string) (*store.UserProfile, error)
	GetTagUsers(ctx context.Context, tagID string, limit int64) ([]string, error)
}

// Service 标签查询服务
type Service struct {
	redis redisStore
}

func NewService(redis *store.RedisStore) *Service {
	return &Service{redis: redis}
}

// GetUserTags 获取单个用户的标签列表
func (s *Service) GetUserTags(ctx context.Context, userID string) (*store.UserTags, error) {
	tags, err := s.redis.GetUserTags(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("lookup user tags: %w", err)
	}
	return tags, nil
}

// GetUserProfile 获取单个用户画像
func (s *Service) GetUserProfile(ctx context.Context, userID string) (*store.UserProfile, error) {
	profile, err := s.redis.GetUserProfile(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("lookup user profile: %w", err)
	}
	return profile, nil
}

// BatchGetUserTags 批量获取用户标签（供 DSP 竞价并发调用，P99 < 10ms）
func (s *Service) BatchGetUserTags(ctx context.Context, userIDs []string) (map[string]*store.UserTags, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}

	type result struct {
		userID string
		tags   *store.UserTags
		err    error
	}

	ch := make(chan result, len(userIDs))

	for _, uid := range userIDs {
		uid := uid
		go func() {
			tags, err := s.redis.GetUserTags(ctx, uid)
			ch <- result{userID: uid, tags: tags, err: err}
		}()
	}

	out := make(map[string]*store.UserTags, len(userIDs))
	var firstErr error
	for range userIDs {
		r := <-ch
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		out[r.userID] = r.tags
	}

	return out, firstErr
}

// GetTagUsers 获取标签下的用户列表
func (s *Service) GetTagUsers(ctx context.Context, tagID string, limit int64) ([]string, error) {
	return s.redis.GetTagUsers(ctx, tagID, limit)
}
