// Package client 提供 adortb-dmp 服务的 Go 客户端，供 DSP 等服务调用。
// DSP 竞价时批量查询用户标签，P99 < 10ms 要求。
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const defaultTimeout = 8 * time.Millisecond // 留 2ms 余量给调用方

// TagScore 单个标签及分值
type TagScore struct {
	TagID string  `json:"tag_id"`
	Score float32 `json:"score"`
}

// UserTags 用户标签数据
type UserTags struct {
	UserID string             `json:"user_id"`
	Tags   map[string]float32 `json:"tags"`
}

// Client DMP 服务客户端
type Client struct {
	endpoint string
	http     *http.Client
}

// New 创建客户端，endpoint 形如 "http://dmp.internal:8086"
func New(endpoint string) *Client {
	return &Client{
		endpoint: endpoint,
		http: &http.Client{
			Timeout: defaultTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 50,
				IdleConnTimeout:     60 * time.Second,
			},
		},
	}
}

// NewWithTimeout 自定义超时创建客户端
func NewWithTimeout(endpoint string, timeout time.Duration) *Client {
	c := New(endpoint)
	c.http.Timeout = timeout
	return c
}

// BatchGetUserTags 批量查询用户标签（供 DSP 竞价并发调用）
// userIDs 最多 200 个
func (c *Client) BatchGetUserTags(ctx context.Context, userIDs []string) (map[string]*UserTags, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}

	body, err := json.Marshal(map[string]interface{}{"user_ids": userIDs})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.endpoint+"/v1/tags/batch", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("batch tags request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dmp error: status %d", resp.StatusCode)
	}

	// 响应格式: { user_id: { tags: {...} } }
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	result := make(map[string]*UserTags, len(raw))
	for uid, data := range raw {
		var ut UserTags
		if err := json.Unmarshal(data, &ut); err != nil {
			continue
		}
		ut.UserID = uid
		result[uid] = &ut
	}
	return result, nil
}

// GetUserTags 获取单个用户标签
func (c *Client) GetUserTags(ctx context.Context, userID string) (*UserTags, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.endpoint+"/v1/user/"+userID+"/tags", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dmp error: status %d", resp.StatusCode)
	}

	var tags UserTags
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return nil, err
	}
	return &tags, nil
}

// ExpandAudience 相似受众扩展
func (c *Client) ExpandAudience(ctx context.Context, seedTags []string, minOverlap, limit int) ([]string, error) {
	body, err := json.Marshal(map[string]interface{}{
		"seed_tags":   seedTags,
		"min_overlap": minOverlap,
		"limit":       limit,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.endpoint+"/v1/audience/expand", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("expand error: status %d", resp.StatusCode)
	}

	var result struct {
		UserIDs []string `json:"user_ids"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.UserIDs, nil
}
