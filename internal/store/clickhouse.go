package store

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/adortb/adortb-dmp/internal/behavior"
)

const (
	chTimeout = 10 * time.Second
)

// ClickHouseClient 通过 HTTP 接口与 ClickHouse 交互
type ClickHouseClient struct {
	endpoint string
	client   *http.Client
}

func NewClickHouseClient(httpEndpoint string) *ClickHouseClient {
	return &ClickHouseClient{
		endpoint: strings.TrimRight(httpEndpoint, "/"),
		client: &http.Client{
			Timeout: chTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        50,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// InsertBehavior 写入行为事件
func (c *ClickHouseClient) InsertBehavior(ctx context.Context, events []*behavior.Event) error {
	if len(events) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO user_behaviors (user_id,event_type,item_id,category,value,timestamp) VALUES ")
	for i, e := range events {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "('%s','%s','%s','%s',%f,'%s')",
			escapeCH(e.UserID),
			escapeCH(string(e.EventType)),
			escapeCH(e.ItemID),
			escapeCH(e.Category),
			e.Value,
			e.Timestamp.UTC().Format("2006-01-02 15:04:05"),
		)
	}

	return c.exec(ctx, sb.String())
}

// InsertTagHistory 写入标签变更历史
func (c *ClickHouseClient) InsertTagHistory(ctx context.Context, userID, tagID string, score float32, action string) error {
	q := fmt.Sprintf(
		"INSERT INTO user_tags_history (user_id,tag_id,score,action,timestamp) VALUES ('%s','%s',%f,'%s','%s')",
		escapeCH(userID), escapeCH(tagID), score, escapeCH(action),
		time.Now().UTC().Format("2006-01-02 15:04:05"),
	)
	return c.exec(ctx, q)
}

// CreateTables 建表（幂等）
func (c *ClickHouseClient) CreateTables(ctx context.Context) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS user_behaviors (
			user_id String,
			event_type String,
			item_id String,
			category String,
			value Float64,
			timestamp DateTime,
			event_date Date MATERIALIZED toDate(timestamp)
		) ENGINE = MergeTree()
		PARTITION BY event_date
		ORDER BY (user_id, timestamp)`,

		`CREATE TABLE IF NOT EXISTS user_tags_history (
			user_id String,
			tag_id String,
			score Float32,
			action String,
			timestamp DateTime
		) ENGINE = MergeTree()
		ORDER BY (user_id, timestamp)`,
	}

	for _, q := range queries {
		if err := c.exec(ctx, q); err != nil {
			return fmt.Errorf("create table: %w", err)
		}
	}
	return nil
}

func (c *ClickHouseClient) exec(ctx context.Context, query string) error {
	reqURL := c.endpoint + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, strings.NewReader(query))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("clickhouse exec: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("clickhouse error %d: %s", resp.StatusCode, body)
	}
	return nil
}

// Query 执行查询并返回原始响应
func (c *ClickHouseClient) Query(ctx context.Context, query string) (string, error) {
	params := url.Values{}
	params.Set("query", query)
	reqURL := c.endpoint + "/?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("clickhouse error %d: %s", resp.StatusCode, body)
	}
	return string(body), nil
}

// escapeCH 简单转义 ClickHouse 字符串（防注入）
func escapeCH(s string) string {
	return strings.NewReplacer("'", "\\'", "\\", "\\\\").Replace(s)
}
