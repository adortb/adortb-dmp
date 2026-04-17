package api

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/adortb/adortb-dmp/internal/behavior"
	"github.com/adortb/adortb-dmp/internal/metrics"
	"github.com/adortb/adortb-dmp/internal/store"
	"github.com/adortb/adortb-dmp/internal/tagging"
)

// Pipeline 行为处理管道：Redis + ClickHouse + 标签引擎
type Pipeline struct {
	redis    *store.RedisStore
	ch       *store.ClickHouseClient
	engine   *tagging.Engine
	logger   *slog.Logger
	counters *metrics.Counters
	// ClickHouse 批量写入缓冲
	chBuf    chan *behavior.Event
}

func NewPipeline(
	redis *store.RedisStore,
	ch *store.ClickHouseClient,
	engine *tagging.Engine,
	logger *slog.Logger,
	counters *metrics.Counters,
) *Pipeline {
	p := &Pipeline{
		redis:    redis,
		ch:       ch,
		engine:   engine,
		logger:   logger,
		counters: counters,
		chBuf:    make(chan *behavior.Event, 1000),
	}
	return p
}

// StartCHFlusher 启动后台 ClickHouse 批量写入 goroutine
func (p *Pipeline) StartCHFlusher(ctx context.Context) {
	go p.flushLoop(ctx)
}

func (p *Pipeline) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	buf := make([]*behavior.Event, 0, 100)
	for {
		select {
		case <-ctx.Done():
			if len(buf) > 0 {
				p.flushCH(context.Background(), buf)
			}
			return

		case e := <-p.chBuf:
			buf = append(buf, e)
			if len(buf) >= 100 {
				p.flushCH(ctx, buf)
				buf = buf[:0]
			}

		case <-ticker.C:
			if len(buf) > 0 {
				p.flushCH(ctx, buf)
				buf = buf[:0]
			}
		}
	}
}

func (p *Pipeline) flushCH(ctx context.Context, events []*behavior.Event) {
	if err := p.ch.InsertBehavior(ctx, events); err != nil {
		p.logger.Error("clickhouse insert failed", "count", len(events), "error", err)
	}
}

// Process 实现 BehaviorProcessor 接口
func (p *Pipeline) Process(ctx context.Context, event *behavior.Event) error {
	// 更新 Redis 用户画像（设备、地理）
	if err := p.updateProfile(ctx, event); err != nil {
		p.logger.Warn("update profile failed", "error", err, "user_id", event.UserID)
	}

	// 应用标签规则
	if err := p.engine.Apply(ctx, event); err != nil {
		return fmt.Errorf("apply tags: %w", err)
	}
	p.counters.TagsApplied.Add(1)

	// 更新事件计数（用于规则引擎的 MinCount 判断）
	p.incrEventCount(ctx, event)

	// 异步写 ClickHouse（非阻塞）
	select {
	case p.chBuf <- event:
	default:
		p.logger.Warn("clickhouse buffer full, dropping event", "user_id", event.UserID)
	}

	return nil
}

func (p *Pipeline) updateProfile(ctx context.Context, event *behavior.Event) error {
	if event.Device == "" && event.Geo == "" {
		return nil
	}
	fields := map[string]string{
		"last_active": strconv.FormatInt(event.Timestamp.Unix(), 10),
	}
	if event.Device != "" {
		fields["device"] = event.Device
	}
	if event.Geo != "" {
		fields["geo"] = event.Geo
	}
	return p.redis.SetUserProfile(ctx, event.UserID, fields)
}

func (p *Pipeline) incrEventCount(ctx context.Context, event *behavior.Event) {
	// 按 eventType+category 存储计数（用于规则匹配）
	key := fmt.Sprintf("user:evcnt:%s:%s:%s", event.UserID, event.EventType, event.Category)
	// 存储当前小时的计数
	hour := event.Timestamp.Format("2006010215")
	_ = p.redis.GetRedisClient().HIncrByFloat(ctx, key, hour, 1)
	_ = p.redis.GetRedisClient().Expire(ctx, key, 8*24*time.Hour) // 8 天 TTL

	// 总计数
	totalKey := "user:evcnt:" + event.UserID + ":total"
	_ = p.redis.GetRedisClient().HIncrByFloat(ctx, totalKey, hour, 1)
	_ = p.redis.GetRedisClient().Expire(ctx, totalKey, 8*24*time.Hour)
}
