package outcomeadapter

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
)

var agentContractKeys = []string{"agent_contract", "agentContract", "grok_agent_contract", "grokAgentContract"}

// TransformAnthropicRequest attaches locally verified receipts to matching
// tool_result blocks and adds per-tool requirements to the gateway contract.
// Calls outside the verifier allow-list are intentionally left unchanged.
func TransformAnthropicRequest(body []byte, store *ToolStore, verifier *Verifier) ([]byte, bool, error) {
	if store == nil || verifier == nil {
		return body, false, nil
	}
	var request map[string]json.RawMessage
	if json.Unmarshal(body, &request) != nil {
		return body, false, nil
	}
	messages, ok := rawJSONArray(request["messages"])
	if !ok {
		return body, false, nil
	}

	requirements := make(map[string]map[string]bool)
	changed := false
	for messageIndex, rawMessage := range messages {
		message, ok := rawJSONObject(rawMessage)
		if !ok {
			continue
		}
		blocks, ok := rawJSONArray(message["content"])
		if !ok {
			continue
		}
		messageChanged := false
		for blockIndex, rawBlock := range blocks {
			block, ok := rawJSONObject(rawBlock)
			if !ok || rawString(block["type"]) != "tool_result" {
				continue
			}
			// A failed tool result cannot be converted into success evidence simply
			// because an old artifact happens to exist locally.
			if raw, exists := block["is_error"]; exists {
				var isError bool
				if json.Unmarshal(raw, &isError) != nil || isError {
					continue
				}
			}
			toolUseID := rawString(block["tool_use_id"])
			call, found := store.Get(toolUseID)
			if !found {
				continue
			}
			receipt, required, enforced := verifier.Verify(call)
			if !enforced {
				continue
			}
			content, contentChanged, err := appendReceiptToAnthropicContent(block["content"], receipt, required)
			if err != nil {
				return body, false, err
			}
			if !contentChanged {
				continue
			}
			block["content"] = content
			encoded, err := json.Marshal(block)
			if err != nil {
				return body, false, err
			}
			blocks[blockIndex] = encoded
			if requirements[toolUseID] == nil {
				requirements[toolUseID] = make(map[string]bool, len(required))
			}
			for _, value := range required {
				requirements[toolUseID][value] = true
			}
			messageChanged = true
		}
		if !messageChanged {
			continue
		}
		encodedBlocks, err := json.Marshal(blocks)
		if err != nil {
			return body, false, err
		}
		message["content"] = encodedBlocks
		encodedMessage, err := json.Marshal(message)
		if err != nil {
			return body, false, err
		}
		messages[messageIndex] = encodedMessage
		changed = true
	}
	if !changed {
		return body, false, nil
	}
	encodedMessages, err := json.Marshal(messages)
	if err != nil {
		return body, false, err
	}
	request["messages"] = encodedMessages
	if err := mergeAgentContract(request, requirements); err != nil {
		return body, false, err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return body, false, err
	}
	return encoded, true, nil
}

// appendReceiptToAnthropicContent carries both the verified receipt and its
// requirements in a fixed JSON envelope. This keeps the evidence bound to the
// original tool_result even when an intermediary normalizes private request
// metadata. It returns changed=false for unsupported content shapes.
func appendReceiptToAnthropicContent(raw json.RawMessage, receipt outcomeReceipt, requirements []string) (json.RawMessage, bool, error) {
	encodedReceipt, err := json.Marshal(outcomeReceiptEnvelope{
		Type:     "grok_agent_outcome_receipt",
		Version:  1,
		Requires: append([]string(nil), requirements...),
		Receipt:  receipt,
	})
	if err != nil {
		return nil, false, err
	}
	blocks := make([]json.RawMessage, 0, 2)
	if len(bytes.TrimSpace(raw)) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		var text string
		if json.Unmarshal(raw, &text) == nil {
			if text != "" {
				original, err := json.Marshal(map[string]string{"type": "text", "text": text})
				if err != nil {
					return nil, false, err
				}
				blocks = append(blocks, original)
			}
		} else if existing, ok := rawJSONArray(raw); ok {
			blocks = append(blocks, existing...)
		} else {
			return raw, false, nil
		}
	}
	receiptBlock, err := json.Marshal(map[string]string{"type": "text", "text": string(encodedReceipt)})
	if err != nil {
		return nil, false, err
	}
	blocks = append(blocks, receiptBlock)
	encoded, err := json.Marshal(blocks)
	if err != nil {
		return nil, false, err
	}
	return encoded, true, nil
}

func mergeAgentContract(request map[string]json.RawMessage, requirements map[string]map[string]bool) error {
	entries := make([]toolReceiptRequirement, 0, len(requirements))
	for toolUseID, values := range requirements {
		required := make([]string, 0, len(values))
		for _, value := range []string{"command_exit_zero", "artifact_exists", "svg_valid", "browser_assertions_passed"} {
			if values[value] {
				required = append(required, value)
			}
		}
		if toolUseID != "" && len(required) > 0 {
			entries = append(entries, toolReceiptRequirement{ToolCallID: toolUseID, Requires: required})
		}
	}
	if len(entries) == 0 {
		return nil
	}

	parent, key, contract, parentIsMetadata := findAgentContractSlot(request)
	existing, _ := rawJSONArray(contract["tool_receipts"])
	for _, entry := range entries {
		encoded, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		existing = append(existing, encoded)
	}
	encodedRequirements, err := json.Marshal(existing)
	if err != nil {
		return err
	}
	contract["tool_receipts"] = encodedRequirements
	encodedContract, err := json.Marshal(contract)
	if err != nil {
		return err
	}
	parent[key] = encodedContract
	if parentIsMetadata {
		encodedMetadata, err := json.Marshal(parent)
		if err != nil {
			return err
		}
		request["metadata"] = encodedMetadata
	}
	return nil
}

func findAgentContractSlot(request map[string]json.RawMessage) (map[string]json.RawMessage, string, map[string]json.RawMessage, bool) {
	if metadata, ok := rawJSONObject(request["metadata"]); ok {
		for _, key := range agentContractKeys {
			if contract, ok := rawContractObject(metadata[key]); ok {
				return metadata, key, contract, true
			}
		}
	}
	for _, key := range agentContractKeys {
		if contract, ok := rawContractObject(request[key]); ok {
			return request, key, contract, false
		}
	}
	// CC Switch keeps gateway-private top-level fields while normalizing the
	// Anthropic metadata object. Prefer the protocol-neutral slot for a new
	// adapter contract; an explicit existing contract above still keeps its
	// original location.
	return request, "grok_agent_contract", make(map[string]json.RawMessage), false
}

func rawContractObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, false
	}
	var encoded string
	if json.Unmarshal(raw, &encoded) == nil {
		raw = []byte(encoded)
	}
	return rawJSONObject(raw)
}

func rawJSONObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return nil, false
	}
	return object, true
}

func rawJSONArray(raw json.RawMessage) ([]json.RawMessage, bool) {
	var values []json.RawMessage
	if json.Unmarshal(raw, &values) != nil {
		return nil, false
	}
	return values, true
}

func rawString(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

type anthropicStreamTracker struct {
	store     *ToolStore
	onTracked func()
	pending   []byte
	calls     map[int]*trackedToolCall
}

type trackedToolCall struct {
	id       string
	name     string
	input    []byte
	hasDelta bool
}

func newAnthropicStreamTracker(store *ToolStore, onTracked func()) *anthropicStreamTracker {
	return &anthropicStreamTracker{store: store, onTracked: onTracked, calls: make(map[int]*trackedToolCall)}
}

func (t *anthropicStreamTracker) Feed(chunk []byte) {
	if t == nil || len(chunk) == 0 {
		return
	}
	t.pending = append(t.pending, chunk...)
	for {
		index := bytes.IndexByte(t.pending, '\n')
		if index < 0 {
			return
		}
		line := bytes.TrimSpace(t.pending[:index])
		t.pending = t.pending[index+1:]
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		t.observe(bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:"))))
	}
}

func (t *anthropicStreamTracker) observe(payload []byte) {
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return
	}
	var event struct {
		Type         string `json:"type"`
		Index        int    `json:"index"`
		ContentBlock struct {
			ID    string          `json:"id"`
			Type  string          `json:"type"`
			Name  string          `json:"name"`
			Input json.RawMessage `json:"input"`
		} `json:"content_block"`
		Delta struct {
			Type        string `json:"type"`
			PartialJSON string `json:"partial_json"`
		} `json:"delta"`
	}
	if json.Unmarshal(payload, &event) != nil {
		return
	}
	switch event.Type {
	case "content_block_start":
		if event.ContentBlock.Type == "tool_use" && event.ContentBlock.ID != "" {
			t.calls[event.Index] = &trackedToolCall{
				id:    event.ContentBlock.ID,
				name:  event.ContentBlock.Name,
				input: append([]byte(nil), event.ContentBlock.Input...),
			}
		}
	case "content_block_delta":
		if event.Delta.Type != "input_json_delta" || event.Delta.PartialJSON == "" {
			return
		}
		call := t.calls[event.Index]
		if call == nil {
			return
		}
		if !call.hasDelta {
			call.input = nil
			call.hasDelta = true
		}
		call.input = append(call.input, event.Delta.PartialJSON...)
	}
}

func (t *anthropicStreamTracker) Finish() {
	if t == nil || t.store == nil {
		return
	}
	// A final SSE event is normally newline-terminated, but accepting a valid
	// trailing data line avoids dropping the last tool call on a clean EOF.
	if line := bytes.TrimSpace(t.pending); bytes.HasPrefix(line, []byte("data:")) {
		t.observe(bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:"))))
	}
	t.pending = nil
	for _, call := range t.calls {
		if call == nil || call.id == "" || !json.Valid(call.input) {
			continue
		}
		t.store.Put(ToolCall{
			ID:        call.id,
			Name:      call.name,
			Input:     append(json.RawMessage(nil), call.input...),
			CreatedAt: time.Now(),
		})
		if t.onTracked != nil {
			t.onTracked()
		}
	}
}
