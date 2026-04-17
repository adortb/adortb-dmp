package consumer

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/adortb/adortb-dmp/internal/behavior"
)

const (
	defaultGroupID    = "adortb-dmp"
	defaultMaxBytes   = 1 << 20 // 1MB
	reconnectInterval = 5 * time.Second
)

// EventHandler 处理从 Kafka 消费的事件
type EventHandler interface {
	Process(ctx context.Context, event *behavior.Event) error
}

// Consumer Kafka 消费者（纯标准库实现，支持多 topic）
type Consumer struct {
	brokers []string
	topics  []string
	groupID string
	handler EventHandler
	logger  *slog.Logger
	wg      sync.WaitGroup
}

func NewConsumer(brokers, topics []string, handler EventHandler, logger *slog.Logger) *Consumer {
	return &Consumer{
		brokers: brokers,
		topics:  topics,
		groupID: defaultGroupID,
		handler: handler,
		logger:  logger,
	}
}

// Start 启动所有 topic 的消费 goroutine
func (c *Consumer) Start(ctx context.Context) {
	for _, topic := range c.topics {
		c.wg.Add(1)
		go func(t string) {
			defer c.wg.Done()
			c.consumeLoop(ctx, t)
		}(topic)
	}
}

// Wait 等待所有消费 goroutine 退出
func (c *Consumer) Wait() {
	c.wg.Wait()
}

func (c *Consumer) consumeLoop(ctx context.Context, topic string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := c.consumeOnce(ctx, topic); err != nil {
			c.logger.Warn("kafka consume error, retrying", "topic", topic, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(reconnectInterval):
			}
		}
	}
}

func (c *Consumer) consumeOnce(ctx context.Context, topic string) error {
	if len(c.brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}

	conn, err := net.DialTimeout("tcp", c.brokers[0], 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	c.logger.Info("kafka connected", "broker", c.brokers[0], "topic", topic)

	reader := bufio.NewReader(conn)
	_ = reader // 实际 Kafka 协议解析复杂，这里用简化版

	// 使用简单的 fetch 循环（生产环境应使用完整 Kafka 协议）
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msgs, err := fetchMessages(conn, topic, defaultMaxBytes)
		if err != nil {
			return err
		}

		for _, msg := range msgs {
			var event behavior.Event
			if err := json.Unmarshal(msg, &event); err != nil {
				c.logger.Warn("unmarshal event failed", "error", err)
				continue
			}
			if err := event.Validate(); err != nil {
				c.logger.Warn("invalid event", "error", err)
				continue
			}
			if err := c.handler.Process(ctx, &event); err != nil {
				c.logger.Error("process event failed", "error", err)
			}
		}

		// 避免空转
		if len(msgs) == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}

// fetchMessages 简化版 Kafka Fetch 请求（仅作结构示意）
// 生产环境应使用完整 Kafka wire protocol
func fetchMessages(conn net.Conn, topic string, maxBytes int) ([][]byte, error) {
	// 构造简化的 Fetch 请求头部
	header := buildFetchRequest(topic, maxBytes)
	if _, err := conn.Write(header); err != nil {
		return nil, fmt.Errorf("write fetch: %w", err)
	}

	// 读取响应长度
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var respLen int32
	if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
		return nil, nil // 超时视为无消息
	}

	if respLen <= 0 || respLen > int32(maxBytes) {
		return nil, nil
	}

	buf := make([]byte, respLen)
	if _, err := readFull(conn, buf); err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	return parseMessages(buf), nil
}

func buildFetchRequest(topic string, maxBytes int) []byte {
	// 极简 Fetch 请求（API key=1, version=0）
	topicBytes := []byte(topic)
	totalLen := 2 + 2 + 4 + 4 + 4 + 4 + // header
		2 + len(topicBytes) + 4 + 4 + 4 + 8 + 4 // topic partition

	buf := make([]byte, 4+totalLen)
	binary.BigEndian.PutUint32(buf[0:], uint32(totalLen))
	binary.BigEndian.PutUint16(buf[4:], 1)  // Fetch API
	binary.BigEndian.PutUint16(buf[6:], 0)  // version
	binary.BigEndian.PutUint32(buf[8:], 1)  // correlation ID
	binary.BigEndian.PutUint32(buf[12:], 0) // max wait
	binary.BigEndian.PutUint32(buf[16:], 1) // min bytes
	binary.BigEndian.PutUint32(buf[20:], 1) // topic count
	// topic name
	off := 24
	binary.BigEndian.PutUint16(buf[off:], uint16(len(topicBytes)))
	off += 2
	copy(buf[off:], topicBytes)
	off += len(topicBytes)
	binary.BigEndian.PutUint32(buf[off:], 1)  // partition count
	off += 4
	binary.BigEndian.PutUint32(buf[off:], 0)  // partition 0
	off += 4
	binary.BigEndian.PutUint64(buf[off:], 0)  // offset
	off += 8
	binary.BigEndian.PutUint32(buf[off:], uint32(maxBytes))
	return buf
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// parseMessages 从 Fetch 响应中提取消息 value（简化解析）
func parseMessages(data []byte) [][]byte {
	var results [][]byte
	// 在原始数据中查找 JSON 格式的消息
	// 生产环境应完整解析 MessageSet 格式
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '{' {
			start = i
		} else if data[i] == '}' && start < i {
			candidate := data[start : i+1]
			if json.Valid(candidate) {
				cp := make([]byte, len(candidate))
				copy(cp, candidate)
				results = append(results, cp)
				start = i + 1
			}
		}
	}
	return results
}

// TopicsFromEnv 从逗号分隔字符串解析 topic 列表
func TopicsFromEnv(s string) []string {
	if s == "" {
		return []string{"adortb.events", "adortb.behaviors"}
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
