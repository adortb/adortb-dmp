package behavior

import (
	"errors"
	"time"
)

// EventType 行为事件类型
type EventType string

const (
	EventView     EventType = "view"
	EventSearch   EventType = "search"
	EventPurchase EventType = "purchase"
	EventClick    EventType = "click"
)

var validEventTypes = map[EventType]struct{}{
	EventView:     {},
	EventSearch:   {},
	EventPurchase: {},
	EventClick:    {},
}

var (
	ErrMissingUserID    = errors.New("user_id is required")
	ErrMissingEventType = errors.New("event_type is required")
	ErrInvalidEventType = errors.New("invalid event_type")
)

// Event 用户行为事件
type Event struct {
	UserID    string    `json:"user_id"`
	EventType EventType `json:"event_type"`
	ItemID    string    `json:"item_id"`
	Category  string    `json:"category"`
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	// 设备信息（可选）
	Device    string `json:"device,omitempty"`
	Geo       string `json:"geo,omitempty"`
}

func (e *Event) Validate() error {
	if e.UserID == "" {
		return ErrMissingUserID
	}
	if e.EventType == "" {
		return ErrMissingEventType
	}
	if _, ok := validEventTypes[e.EventType]; !ok {
		return ErrInvalidEventType
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}
	return nil
}
