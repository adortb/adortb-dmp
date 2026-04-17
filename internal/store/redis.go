package store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adortb/adortb-dmp/internal/tagging"
)

const (
	userTagsPrefix    = "user:tags:"
	userProfilePrefix = "user:profile:"
	tagUsersPrefix    = "tag:users:"
	tagMetaPrefix     = "tag:meta:"
	tagTTL            = 30 * 24 * time.Hour // 30 天
)

// UserTags 用户标签列表
type UserTags struct {
	UserID string
	Tags   map[string]float32 // tagID -> score
}

// UserProfile 用户画像
type UserProfile struct {
	UserID     string
	AgeBucket  string
	Gender     string
	Device     string
	Geo        string
	LastActive time.Time
}

// RedisClient 简单 Redis 客户端（标准库实现，无外部依赖）
type RedisClient struct {
	addr string
	pool *connPool
}

func NewRedisClient(addr string, poolSize int) *RedisClient {
	return &RedisClient{
		addr: addr,
		pool: newConnPool(addr, poolSize),
	}
}

// HIncrByFloat 对 HASH 字段做浮点自增
func (c *RedisClient) HIncrByFloat(ctx context.Context, key, field string, delta float64) error {
	conn, err := c.pool.get(ctx)
	if err != nil {
		return err
	}
	defer c.pool.put(conn)

	cmd := fmt.Sprintf("*4\r\n$12\r\nHINCRBYFLOAT\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%s\r\n%s\r\n",
		len(key), key, len(field), field, lenStr(delta), formatFloat(delta))
	if _, err := conn.WriteRead(ctx, cmd); err != nil {
		return err
	}
	return nil
}

// HGetAll 获取 HASH 所有字段
func (c *RedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	conn, err := c.pool.get(ctx)
	if err != nil {
		return nil, err
	}
	defer c.pool.put(conn)

	cmd := fmt.Sprintf("*2\r\n$7\r\nHGETALL\r\n$%d\r\n%s\r\n", len(key), key)
	resp, err := conn.WriteRead(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return parseHGetAll(resp), nil
}

// HSet 设置 HASH 字段
func (c *RedisClient) HSet(ctx context.Context, key string, fields map[string]string) error {
	conn, err := c.pool.get(ctx)
	if err != nil {
		return err
	}
	defer c.pool.put(conn)

	args := make([]string, 0, 2+len(fields)*2)
	args = append(args, "HSET", key)
	for k, v := range fields {
		args = append(args, k, v)
	}
	cmd := buildCmd(args...)
	if _, err := conn.WriteRead(ctx, cmd); err != nil {
		return err
	}
	return nil
}

// Expire 设置 key 过期时间
func (c *RedisClient) Expire(ctx context.Context, key string, ttl time.Duration) error {
	conn, err := c.pool.get(ctx)
	if err != nil {
		return err
	}
	defer c.pool.put(conn)

	secs := int64(ttl.Seconds())
	cmd := buildCmd("EXPIRE", key, strconv.FormatInt(secs, 10))
	if _, err := conn.WriteRead(ctx, cmd); err != nil {
		return err
	}
	return nil
}

// SAdd 向 SET 添加成员
func (c *RedisClient) SAdd(ctx context.Context, key string, members ...string) error {
	conn, err := c.pool.get(ctx)
	if err != nil {
		return err
	}
	defer c.pool.put(conn)

	args := make([]string, 0, 2+len(members))
	args = append(args, "SADD", key)
	args = append(args, members...)
	cmd := buildCmd(args...)
	if _, err := conn.WriteRead(ctx, cmd); err != nil {
		return err
	}
	return nil
}

// SMembers 获取 SET 成员
func (c *RedisClient) SMembers(ctx context.Context, key string) ([]string, error) {
	conn, err := c.pool.get(ctx)
	if err != nil {
		return nil, err
	}
	defer c.pool.put(conn)

	cmd := buildCmd("SMEMBERS", key)
	resp, err := conn.WriteRead(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return parseArray(resp), nil
}

// ZAdd 向 ZSET 添加成员
func (c *RedisClient) ZAdd(ctx context.Context, key string, score float64, member string) error {
	conn, err := c.pool.get(ctx)
	if err != nil {
		return err
	}
	defer c.pool.put(conn)

	cmd := buildCmd("ZADD", key, formatFloat(score), member)
	if _, err := conn.WriteRead(ctx, cmd); err != nil {
		return err
	}
	return nil
}

// ZRevRangeByScore 按分值倒序获取 ZSET 成员
func (c *RedisClient) ZRevRangeByScore(ctx context.Context, key string, limit int64) ([]string, error) {
	conn, err := c.pool.get(ctx)
	if err != nil {
		return nil, err
	}
	defer c.pool.put(conn)

	cmd := buildCmd("ZREVRANGEBYSCORE", key, "+inf", "-inf", "LIMIT", "0", strconv.FormatInt(limit, 10))
	resp, err := conn.WriteRead(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return parseArray(resp), nil
}

// ZIncrBy 对 ZSET 成员分值做增量
func (c *RedisClient) ZIncrBy(ctx context.Context, key string, delta float64, member string) error {
	conn, err := c.pool.get(ctx)
	if err != nil {
		return err
	}
	defer c.pool.put(conn)

	cmd := buildCmd("ZINCRBY", key, formatFloat(delta), member)
	if _, err := conn.WriteRead(ctx, cmd); err != nil {
		return err
	}
	return nil
}

// RedisStore 实现 tagging.TagStore
type RedisStore struct {
	client *RedisClient
}

func NewRedisStore(client *RedisClient) *RedisStore {
	return &RedisStore{client: client}
}

// GetRedisClient 暴露底层客户端（供需要直接操作 Redis 的模块使用）
func (s *RedisStore) GetRedisClient() *RedisClient {
	return s.client
}

func (s *RedisStore) IncrTagScore(ctx context.Context, userID, tagID string, delta float32) error {
	key := userTagsPrefix + userID
	if err := s.client.HIncrByFloat(ctx, key, tagID, float64(delta)); err != nil {
		return fmt.Errorf("incr tag score: %w", err)
	}
	// 刷新 TTL
	_ = s.client.Expire(ctx, key, tagTTL)
	// 更新反向索引
	reverseKey := tagUsersPrefix + tagID
	_ = s.client.ZIncrBy(ctx, reverseKey, float64(delta), userID)
	return nil
}

func (s *RedisStore) GetEventCount(ctx context.Context, userID string, cond tagging.Condition) (int, error) {
	// 简化：用 HASH 存储事件类型+类目计数
	key := fmt.Sprintf("user:evcnt:%s:%s:%s", userID, cond.EventType, cond.Category)
	m, err := s.client.HGetAll(ctx, key)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, v := range m {
		if n, err := strconv.Atoi(v); err == nil {
			count += n
		}
	}
	return count, nil
}

func (s *RedisStore) GetTotalEventCount(ctx context.Context, userID string, cond tagging.Condition) (int, error) {
	key := "user:evcnt:" + userID + ":total"
	m, err := s.client.HGetAll(ctx, key)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, v := range m {
		if n, err := strconv.Atoi(v); err == nil {
			count += n
		}
	}
	return count, nil
}

// GetUserTags 获取用户标签
func (s *RedisStore) GetUserTags(ctx context.Context, userID string) (*UserTags, error) {
	key := userTagsPrefix + userID
	m, err := s.client.HGetAll(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get user tags: %w", err)
	}
	tags := make(map[string]float32, len(m))
	for k, v := range m {
		if f, err := strconv.ParseFloat(v, 32); err == nil {
			tags[k] = float32(f)
		}
	}
	return &UserTags{UserID: userID, Tags: tags}, nil
}

// GetUserProfile 获取用户画像
func (s *RedisStore) GetUserProfile(ctx context.Context, userID string) (*UserProfile, error) {
	key := userProfilePrefix + userID
	m, err := s.client.HGetAll(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get user profile: %w", err)
	}
	p := &UserProfile{UserID: userID}
	if v, ok := m["age_bucket"]; ok {
		p.AgeBucket = v
	}
	if v, ok := m["gender"]; ok {
		p.Gender = v
	}
	if v, ok := m["device"]; ok {
		p.Device = v
	}
	if v, ok := m["geo"]; ok {
		p.Geo = v
	}
	if v, ok := m["last_active"]; ok {
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
			p.LastActive = time.Unix(ts, 0)
		}
	}
	return p, nil
}

// SetUserProfile 更新用户画像字段
func (s *RedisStore) SetUserProfile(ctx context.Context, userID string, fields map[string]string) error {
	key := userProfilePrefix + userID
	return s.client.HSet(ctx, key, fields)
}

// GetTagUsers 获取标签对应用户（ZSET 倒序，按分值）
func (s *RedisStore) GetTagUsers(ctx context.Context, tagID string, limit int64) ([]string, error) {
	key := tagUsersPrefix + tagID
	return s.client.ZRevRangeByScore(ctx, key, limit)
}

// ---- 连接池实现 ----

type redisConn struct {
	conn net.Conn
	buf  []byte
}

func (c *redisConn) WriteRead(ctx context.Context, cmd string) (string, error) {
	deadline, ok := ctx.Deadline()
	if ok {
		_ = c.conn.SetDeadline(deadline)
	} else {
		_ = c.conn.SetDeadline(time.Now().Add(5 * time.Second))
	}

	if _, err := c.conn.Write([]byte(cmd)); err != nil {
		return "", fmt.Errorf("redis write: %w", err)
	}

	c.buf = c.buf[:0]
	tmp := make([]byte, 4096)
	for {
		n, err := c.conn.Read(tmp)
		if n > 0 {
			c.buf = append(c.buf, tmp[:n]...)
		}
		if err != nil {
			break
		}
		// 简单判断响应是否完整（以 \r\n 结尾）
		if len(c.buf) > 0 && c.buf[len(c.buf)-1] == '\n' {
			break
		}
	}
	return string(c.buf), nil
}

type connPool struct {
	addr string
	ch   chan *redisConn
	mu   sync.Mutex
}

func newConnPool(addr string, size int) *connPool {
	return &connPool{
		addr: addr,
		ch:   make(chan *redisConn, size),
	}
}

func (p *connPool) get(ctx context.Context) (*redisConn, error) {
	select {
	case c := <-p.ch:
		return c, nil
	default:
	}
	conn, err := net.DialTimeout("tcp", p.addr, 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("redis connect: %w", err)
	}
	return &redisConn{conn: conn, buf: make([]byte, 0, 4096)}, nil
}

func (p *connPool) put(c *redisConn) {
	if c == nil {
		return
	}
	select {
	case p.ch <- c:
	default:
		_ = c.conn.Close()
	}
}

// ---- 辅助函数 ----

func buildCmd(args ...string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return sb.String()
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func lenStr(f float64) string {
	return strconv.Itoa(len(formatFloat(f)))
}

func parseHGetAll(resp string) map[string]string {
	result := make(map[string]string)
	lines := strings.Split(resp, "\r\n")
	var key string
	expectKey := true
	for _, line := range lines {
		if line == "" || line[0] == '*' || line[0] == ':' {
			continue
		}
		if line[0] == '$' {
			continue // 跳过长度行
		}
		if line[0] == '-' {
			return result
		}
		if expectKey {
			key = line
			expectKey = false
		} else {
			result[key] = line
			expectKey = true
		}
	}
	return result
}

func parseArray(resp string) []string {
	var result []string
	lines := strings.Split(resp, "\r\n")
	for _, line := range lines {
		if line == "" || line[0] == '*' || line[0] == '$' || line[0] == ':' || line[0] == '-' {
			continue
		}
		result = append(result, line)
	}
	return result
}

var ErrNilRedis = errors.New("redis: nil")
