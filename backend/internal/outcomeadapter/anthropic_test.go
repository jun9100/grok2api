package outcomeadapter

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
)

func TestTransformAnthropicRequestAddsBoundSVGReceipt(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "pelican.svg")
	writeTestFile(t, path, `<svg xmlns="http://www.w3.org/2000/svg"><defs><path id="beak"/></defs><use href="#beak"/></svg>`)

	store := NewToolStore(0)
	store.Put(ToolCall{ID: "toolu_write", Name: "Write", Input: json.RawMessage(`{"file_path":"pelican.svg"}`)})
	verifier, err := NewVerifier([]string{root}, root)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{
  "metadata":{"request_tag":"keep"},
  "messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_write","is_error":false,"content":"file written"}]}]
}`)

	transformed, changed, err := TransformAnthropicRequest(body, store, verifier)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("valid local result was not transformed")
	}

	var request struct {
		Metadata map[string]json.RawMessage `json:"metadata"`
		Messages []struct {
			Content []struct {
				Type      string          `json:"type"`
				ToolUseID string          `json:"tool_use_id"`
				Content   json.RawMessage `json:"content"`
			} `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(transformed, &request); err != nil {
		t.Fatal(err)
	}
	if got := string(request.Metadata["request_tag"]); got != `"keep"` {
		t.Fatalf("metadata was not preserved: %s", got)
	}
	var rawRequest map[string]json.RawMessage
	if err := json.Unmarshal(transformed, &rawRequest); err != nil {
		t.Fatal(err)
	}
	contract, ok := rawJSONObject(rawRequest["grok_agent_contract"])
	if !ok {
		t.Fatalf("missing adapter contract: %s", transformed)
	}
	var requirements []toolReceiptRequirement
	if err := json.Unmarshal(contract["tool_receipts"], &requirements); err != nil {
		t.Fatal(err)
	}
	if want := []toolReceiptRequirement{{ToolCallID: "toolu_write", Requires: []string{"artifact_exists", "svg_valid"}}}; !reflect.DeepEqual(requirements, want) {
		t.Fatalf("tool receipt requirements = %#v, want %#v", requirements, want)
	}

	content := request.Messages[0].Content[0].Content
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(content, &blocks); err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 || blocks[0].Text != "file written" {
		t.Fatalf("tool result content = %#v", blocks)
	}
	var envelope outcomeReceiptEnvelope
	if err := json.Unmarshal([]byte(blocks[1].Text), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Type != "grok_agent_outcome_receipt" || envelope.Version != 1 || !reflect.DeepEqual(envelope.Requires, []string{"artifact_exists", "svg_valid"}) {
		t.Fatalf("receipt envelope = %#v", envelope)
	}
	if envelope.Receipt.Artifact == nil || !envelope.Receipt.Artifact.Exists || envelope.Receipt.SVG == nil || !envelope.Receipt.SVG.Valid || !envelope.Receipt.SVG.ReferencesValid {
		t.Fatalf("receipt = %#v", envelope.Receipt)
	}
}

func TestTransformAnthropicRequestLeavesUnsafeOrUnsupportedResultsUntouched(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "result.txt")
	writeTestFile(t, path, "already here")
	verifier, err := NewVerifier([]string{root}, root)
	if err != nil {
		t.Fatal(err)
	}

	for name, body := range map[string][]byte{
		"failed tool result":  []byte(`{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_write","is_error":true,"content":"write failed"}]}]}`),
		"unsupported content": []byte(`{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_write","is_error":false,"content":{"unexpected":"object"}}]}]}`),
	} {
		t.Run(name, func(t *testing.T) {
			store := NewToolStore(0)
			store.Put(ToolCall{ID: "toolu_write", Name: "Write", Input: json.RawMessage(`{"file_path":"result.txt"}`)})
			transformed, changed, err := TransformAnthropicRequest(body, store, verifier)
			if err != nil {
				t.Fatal(err)
			}
			if changed || !bytes.Equal(transformed, body) {
				t.Fatalf("unsafe input was changed: %s", transformed)
			}
		})
	}
}

func TestAnthropicStreamTrackerRecordsFragmentedFinalEvent(t *testing.T) {
	store := NewToolStore(0)
	tracker := newAnthropicStreamTracker(store, nil)
	stream := "data: {\"type\":\"content_block_start\",\"index\":4,\"content_block\":{\"id\":\"toolu_final\",\"type\":\"tool_use\",\"name\":\"Write\",\"input\":{}}}\n" +
		`data: {"type":"content_block_delta","index":4,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"output.svg\"}"}}`
	for _, chunk := range [][]byte{[]byte(stream[:23]), []byte(stream[23:97]), []byte(stream[97:])} {
		tracker.Feed(chunk)
	}
	tracker.Finish()

	call, ok := store.Get("toolu_final")
	if !ok {
		t.Fatal("final non-newline SSE event was not stored")
	}
	if call.Name != "Write" || string(call.Input) != `{"file_path":"output.svg"}` {
		t.Fatalf("tracked call = %#v", call)
	}
}

func TestTransformAnthropicRequestWithoutLocalVerifierIsNoOp(t *testing.T) {
	body := []byte(`{"messages":[]}`)
	if transformed, changed, err := TransformAnthropicRequest(body, nil, nil); err != nil || changed || !bytes.Equal(transformed, body) {
		t.Fatalf("nil dependencies transformed request: body=%s changed=%t err=%v", transformed, changed, err)
	}
}
