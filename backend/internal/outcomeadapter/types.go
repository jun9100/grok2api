// Package outcomeadapter provides a local, opt-in bridge between a tool-using
// client and the gateway's strict agent outcome contract.
package outcomeadapter

import (
	"encoding/json"
	"strings"
	"sync"
	"time"
)

const defaultToolCallTTL = 30 * time.Minute

// ToolCall is the minimum local fact needed to verify an artifact after the
// client has executed the corresponding tool. Input is kept in memory only.
type ToolCall struct {
	ID        string
	Name      string
	Input     json.RawMessage
	CreatedAt time.Time
}

// ToolStore keeps recently emitted tool calls long enough for their next
// tool_result request. It has no persistence and is safe for concurrent proxy
// requests.
type ToolStore struct {
	mu    sync.Mutex
	ttl   time.Duration
	calls map[string]ToolCall
}

// NewToolStore creates an in-memory tool-call ledger.
func NewToolStore(ttl time.Duration) *ToolStore {
	if ttl <= 0 {
		ttl = defaultToolCallTTL
	}
	return &ToolStore{ttl: ttl, calls: make(map[string]ToolCall)}
}

// Put records a tool call only when it has a non-empty ID.
func (s *ToolStore) Put(call ToolCall) {
	if s == nil {
		return
	}
	call.ID = strings.TrimSpace(call.ID)
	if call.ID == "" {
		return
	}
	if call.CreatedAt.IsZero() {
		call.CreatedAt = time.Now()
	}
	call.Input = append(json.RawMessage(nil), call.Input...)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	s.calls[call.ID] = call
}

// Get returns a copy so request transforms cannot mutate the shared ledger.
func (s *ToolStore) Get(id string) (ToolCall, bool) {
	if s == nil {
		return ToolCall{}, false
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return ToolCall{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	call, ok := s.calls[id]
	if !ok {
		return ToolCall{}, false
	}
	call.Input = append(json.RawMessage(nil), call.Input...)
	return call, true
}

func (s *ToolStore) pruneLocked(now time.Time) {
	for id, call := range s.calls {
		if now.Sub(call.CreatedAt) > s.ttl {
			delete(s.calls, id)
		}
	}
}

type artifactReceipt struct {
	Exists bool `json:"exists"`
}

type svgReceipt struct {
	Valid           bool `json:"valid"`
	ReferencesValid bool `json:"references_valid"`
}

type outcomeReceipt struct {
	Artifact *artifactReceipt `json:"artifact,omitempty"`
	SVG      *svgReceipt      `json:"svg,omitempty"`
}

type outcomeReceiptEnvelope struct {
	Type     string         `json:"type"`
	Version  int            `json:"version"`
	Requires []string       `json:"requires"`
	Receipt  outcomeReceipt `json:"receipt"`
}

type toolReceiptRequirement struct {
	ToolCallID string   `json:"tool_call_id"`
	Requires   []string `json:"requires"`
}
