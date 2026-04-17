package tagging

import (
	"context"
	"sync"

	"github.com/adortb/adortb-dmp/internal/behavior"
)

// TagStore 标签存储接口
type TagStore interface {
	// IncrTagScore 增加用户标签分值（增量）
	IncrTagScore(ctx context.Context, userID, tagID string, delta float32) error
	// GetEventCount 获取时间窗口内事件计数
	GetEventCount(ctx context.Context, userID string, cond Condition) (int, error)
	// GetTotalEventCount 获取用户总事件数（用于活跃度规则）
	GetTotalEventCount(ctx context.Context, userID string, cond Condition) (int, error)
}

// Engine 标签规则引擎
type Engine struct {
	rules []Rule
	store TagStore
	mu    sync.RWMutex
}

func NewEngine(store TagStore, rules []Rule) *Engine {
	return &Engine{
		rules: rules,
		store: store,
	}
}

// Apply 对单条行为事件应用所有规则，异步打标签
func (e *Engine) Apply(ctx context.Context, event *behavior.Event) error {
	e.mu.RLock()
	rules := make([]Rule, len(e.rules))
	copy(rules, e.rules)
	e.mu.RUnlock()

	for _, rule := range rules {
		if err := e.applyRule(ctx, event, rule); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) applyRule(ctx context.Context, event *behavior.Event, rule Rule) error {
	for _, cond := range rule.Conditions {
		// 单次价值检查（无需历史，直接判断）
		if cond.MinValue > 0 {
			if !cond.MatchEvent(event) {
				return nil
			}
			// 满足高价值条件，立即打标
			return e.store.IncrTagScore(ctx, event.UserID, rule.TagID, rule.Weight)
		}

		// 需要历史计数的条件
		if !cond.MatchEvent(event) {
			continue
		}

		var count int
		var err error

		if cond.EventType == "" {
			// 活跃度：统计所有事件
			count, err = e.store.GetTotalEventCount(ctx, event.UserID, cond)
		} else {
			count, err = e.store.GetEventCount(ctx, event.UserID, cond)
		}
		if err != nil {
			return err
		}

		if count >= cond.MinCount {
			return e.store.IncrTagScore(ctx, event.UserID, rule.TagID, rule.Weight)
		}
	}
	return nil
}

// AddRule 动态添加规则（线程安全）
func (e *Engine) AddRule(rule Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = append(e.rules, rule)
}

// Rules 返回当前规则列表副本
func (e *Engine) Rules() []Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Rule, len(e.rules))
	copy(out, e.rules)
	return out
}
