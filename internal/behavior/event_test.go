package behavior

import (
	"testing"
	"time"
)

func TestEventValidate(t *testing.T) {
	tests := []struct {
		name    string
		event   Event
		wantErr error
	}{
		{
			name:    "valid impression",
			event:   Event{UserID: "u1", EventType: EventView, ItemID: "i1"},
			wantErr: nil,
		},
		{
			name:    "missing user_id",
			event:   Event{EventType: EventView},
			wantErr: ErrMissingUserID,
		},
		{
			name:    "missing event_type",
			event:   Event{UserID: "u1"},
			wantErr: ErrMissingEventType,
		},
		{
			name:    "invalid event_type",
			event:   Event{UserID: "u1", EventType: "unknown"},
			wantErr: ErrInvalidEventType,
		},
		{
			name:    "all valid event types",
			event:   Event{UserID: "u1", EventType: EventPurchase, Value: 99.9},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.event.Validate()
			if err != tc.wantErr {
				t.Errorf("Validate() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestEventValidateTimestamp(t *testing.T) {
	e := Event{UserID: "u1", EventType: EventClick}
	before := time.Now()
	_ = e.Validate()
	if e.Timestamp.Before(before) {
		t.Error("Validate should set Timestamp when zero")
	}
}

func TestValidEventTypes(t *testing.T) {
	types := []EventType{EventView, EventSearch, EventPurchase, EventClick}
	for _, et := range types {
		e := Event{UserID: "u1", EventType: et}
		if err := e.Validate(); err != nil {
			t.Errorf("event type %s should be valid, got %v", et, err)
		}
	}
}
