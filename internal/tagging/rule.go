package tagging

import (
	"time"

	"github.com/adortb/adortb-dmp/internal/behavior"
)

// Condition 规则触发条件
type Condition struct {
	EventType  behavior.EventType
	Category   string  // 空表示不限类目
	MinCount   int     // 时间窗口内最少触发次数
	MinValue   float64 // 单次行为最低价值（0 表示不限）
	TimeWindow time.Duration
}

// Rule 标签规则
type Rule struct {
	ID         string
	TagID      string
	Name       string
	Conditions []Condition
	Weight     float32
}

// Match 检查单条事件是否满足条件（用于单事件快速匹配）
func (c *Condition) MatchEvent(e *behavior.Event) bool {
	if c.EventType != "" && e.EventType != c.EventType {
		return false
	}
	if c.Category != "" && e.Category != c.Category {
		return false
	}
	if c.MinValue > 0 && e.Value < c.MinValue {
		return false
	}
	// MinCount 需要历史统计，单事件不做计数检查
	return true
}

// DefaultRules 内置 10 条基础规则
var DefaultRules = []Rule{
	// 电商兴趣标签（访问次数触发，时间窗口 7 天）
	{
		ID:    "rule:interest:electronics",
		TagID: "interest:electronics",
		Name:  "电子产品兴趣",
		Conditions: []Condition{
			{EventType: behavior.EventView, Category: "electronics", MinCount: 5, TimeWindow: 7 * 24 * time.Hour},
		},
		Weight: 0.8,
	},
	{
		ID:    "rule:interest:clothing",
		TagID: "interest:clothing",
		Name:  "服装兴趣",
		Conditions: []Condition{
			{EventType: behavior.EventView, Category: "clothing", MinCount: 5, TimeWindow: 7 * 24 * time.Hour},
		},
		Weight: 0.8,
	},
	{
		ID:    "rule:interest:food",
		TagID: "interest:food",
		Name:  "食品兴趣",
		Conditions: []Condition{
			{EventType: behavior.EventView, Category: "food", MinCount: 5, TimeWindow: 7 * 24 * time.Hour},
		},
		Weight: 0.7,
	},
	{
		ID:    "rule:interest:sports",
		TagID: "interest:sports",
		Name:  "运动兴趣",
		Conditions: []Condition{
			{EventType: behavior.EventView, Category: "sports", MinCount: 5, TimeWindow: 7 * 24 * time.Hour},
		},
		Weight: 0.7,
	},
	// 高价值用户
	{
		ID:    "rule:high_value",
		TagID: "high_value_user",
		Name:  "高价值用户",
		Conditions: []Condition{
			{EventType: behavior.EventPurchase, MinValue: 100},
		},
		Weight: 1.0,
	},
	// 购买意向
	{
		ID:    "rule:purchase_intent",
		TagID: "purchase_intent",
		Name:  "购买意向",
		Conditions: []Condition{
			{EventType: behavior.EventSearch, MinCount: 3, TimeWindow: 24 * time.Hour},
		},
		Weight: 0.9,
	},
	// 活跃用户（7 天内 ≥10 事件）
	{
		ID:    "rule:active_user",
		TagID: "active_user",
		Name:  "活跃用户",
		Conditions: []Condition{
			{MinCount: 10, TimeWindow: 7 * 24 * time.Hour},
		},
		Weight: 0.6,
	},
	// 设备标签（无计数要求，单次即打标）
	{
		ID:    "rule:device:ios",
		TagID: "device:ios",
		Name:  "iOS 设备",
		Conditions: []Condition{
			{EventType: behavior.EventView, MinCount: 1, TimeWindow: 30 * 24 * time.Hour},
		},
		Weight: 0.5,
	},
	{
		ID:    "rule:device:android",
		TagID: "device:android",
		Name:  "Android 设备",
		Conditions: []Condition{
			{EventType: behavior.EventView, MinCount: 1, TimeWindow: 30 * 24 * time.Hour},
		},
		Weight: 0.5,
	},
	// 搜索兴趣
	{
		ID:    "rule:search_active",
		TagID: "search_active",
		Name:  "搜索活跃",
		Conditions: []Condition{
			{EventType: behavior.EventSearch, MinCount: 5, TimeWindow: 7 * 24 * time.Hour},
		},
		Weight: 0.6,
	},
}
