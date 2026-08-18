package gateway

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

func TestAgentOutcomeRequirementResponsesLedger(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "metadata":{"agent_contract":{"requires":["mutation","verification"]}},
  "input":[
    {"type":"function_call","call_id":"write-1","name":"write_file","arguments":"{\"path\":\"/tmp/a.txt\",\"content\":\"ok\"}"},
    {"type":"function_call_output","call_id":"write-1","status":"completed","output":{"exit_code":0}},
    {"type":"shell_call","call_id":"test-1","action":{"commands":["go test ./..."]}},
    {"type":"shell_call_output","call_id":"test-1","status":"completed","output":[{"outcome":{"type":"exit","exit_code":0}}]}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolResponses, 6)
	if !requirement.Enabled || !requirement.ContractOpen || !requirement.RequiresMutation || !requirement.RequiresVerification {
		t.Fatalf("contract requirement = %#v", requirement)
	}
	if !requirement.VerifiedWrite || !requirement.VerifiedExecution || !requirement.VerifiedVerification {
		t.Fatalf("Responses ledger did not retain completed tool outcomes: %#v", requirement)
	}

	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"type":"response.reasoning_text.delta","delta":"checking"}`,
			`data: {"type":"response.output_text.delta","delta":"done"}`,
			`data: {"type":"response.completed","response":{"id":"resp_1"}}`,
		))),
		qualityProtocolResponses,
		QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond},
		requirement,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if verdict != QualityDeliver || code != "" {
		t.Fatalf("completed Responses contract = verdict=%s code=%q", verdict, code)
	}
}

func TestStripAgentOutcomeMetadataLeavesProviderPayloadValid(t *testing.T) {
	t.Parallel()
	original := []byte(`{
  "model":"grok-4.6",
  "metadata":{"request_tag":"keep","agent_contract":"{\"requires\":[\"mutation\"]}"},
  "grok_agent_contract":{"requires":["verification"]},
  "input":"hello"
}`)
	stripped := stripAgentOutcomeMetadata(original)
	if strings.Contains(string(stripped), "agent_contract") || strings.Contains(string(stripped), "grok_agent_contract") {
		t.Fatalf("internal contract leaked upstream: %s", stripped)
	}
	var payload map[string]any
	if err := json.Unmarshal(stripped, &payload); err != nil {
		t.Fatal(err)
	}
	metadata := payload["metadata"].(map[string]any)
	if metadata["request_tag"] != "keep" || payload["input"] != "hello" {
		t.Fatalf("provider payload changed unexpectedly: %#v", payload)
	}

	plain := []byte(`{"model":"grok-4.6","input":"hello"}`)
	if got := stripAgentOutcomeMetadata(plain); string(got) != string(plain) {
		t.Fatalf("request without contract was needlessly rewritten: %s", got)
	}
}

func TestAgentOutcomeGuardWithholdsUnfulfilledResponsesContract(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "metadata":{"grok_agent_contract":{"requires":["mutation"]}},
  "input":[
    {"type":"function_call","call_id":"write-1","name":"write_file","arguments":"{}"},
    {"type":"function_call_output","call_id":"write-1","status":"failed","output":{"exit_code":1}}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolResponses, 6)
	if !requirement.Enabled || requirement.VerifiedWrite {
		t.Fatalf("failed result unexpectedly satisfied mutation contract: %#v", requirement)
	}

	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"type":"response.reasoning_text.delta","delta":"I will write it."}`,
			`data: {"type":"response.output_text.delta","delta":"The file is ready."}`,
			`data: {"type":"response.completed"}`,
		))),
		qualityProtocolResponses,
		QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond},
		requirement,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = replay.Close()
	if verdict != QualityWithhold || code != ErrorAgentContractUnfulfilled {
		t.Fatalf("unfulfilled contract = verdict=%s code=%q", verdict, code)
	}
}

func TestAgentOutcomeGuardRecoversFailedMutationWithoutContract(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"write-1","type":"function","function":{"name":"Write","arguments":"{\"path\":\"/tmp/a.txt\"}"}}]},
    {"role":"tool","tool_call_id":"write-1","content":"{\"exit_code\":1}"}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolChat, 6)
	if !requirement.Enabled || !requirement.ContractOpen || !requirement.RequiresMutation || requirement.VerifiedWrite {
		t.Fatalf("implicit failed-mutation recovery = %#v", requirement)
	}

	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"choices":[{"delta":{"content":"The file is ready."},"finish_reason":"stop"}]}`,
		))),
		qualityProtocolChat,
		QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond},
		requirement,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = replay.Close()
	if verdict != QualityWithhold || code != ErrorAgentContractUnfulfilled {
		t.Fatalf("implicit failed-mutation recovery = verdict=%s code=%q", verdict, code)
	}
}

func TestAgentOutcomeRequirementChatLedger(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "metadata":{"agent_contract":{"requires":["mutation","execution","verification"]}},
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"write-1","type":"function","function":{"name":"Write","arguments":"{\"path\":\"/tmp/a.txt\"}"}}]},
    {"role":"tool","tool_call_id":"write-1","content":"{\"exit_code\":0}"},
    {"role":"assistant","tool_calls":[{"id":"test-1","type":"function","function":{"name":"shell_command","arguments":"{\"command\":\"go test ./...\"}"}}]},
    {"role":"tool","tool_call_id":"test-1","content":"{\"outcome\":{\"type\":\"exit\",\"exit_code\":0}}"}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolChat, 6)
	if !requirement.Enabled || !requirement.VerifiedWrite || !requirement.VerifiedExecution || !requirement.VerifiedVerification {
		t.Fatalf("Chat ledger = %#v", requirement)
	}
}

func TestAgentOutcomeRequirementAnthropicLedger(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "metadata":{"agent_contract":{"requires":["mutation"]}},
  "messages":[
    {"role":"assistant","content":[{"type":"tool_use","id":"write-1","name":"Write","input":{"file_path":"/tmp/a.txt","content":"ok"}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"write-1","is_error":false,"content":"ok"}]}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolAnthropic, 6)
	if !requirement.Enabled || !requirement.VerifiedWrite || !requirement.contractSatisfied() {
		t.Fatalf("Anthropic ledger = %#v", requirement)
	}
}

func TestAgentOutcomeGuardDetectsRepeatedObservationOnlyContinuation(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "metadata":{"agent_contract":{"requires":["mutation"]}},
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"read-1","type":"function","function":{"name":"Read","arguments":"{\"path\":\"a.txt\"}"}}]},
    {"role":"tool","tool_call_id":"read-1","content":"ok"},
    {"role":"assistant","tool_calls":[{"id":"read-2","type":"function","function":{"name":"Read","arguments":"{\"path\":\"b.txt\"}"}}]},
    {"role":"tool","tool_call_id":"read-2","content":"ok"}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolChat, 3)
	if !requirement.StallSuspected || requirement.ObservationActions != 2 {
		t.Fatalf("observation history = %#v", requirement)
	}

	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"id":"chat_1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"read-3","type":"function","function":{"name":"Read","arguments":"{\"path\":\"c.txt\"}"}}]},"finish_reason":"tool_calls"}]}`,
		))),
		qualityProtocolChat,
		QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond, AgentStallTurns: 3},
		requirement,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = replay.Close()
	if verdict != QualityWithhold || code != ErrorAgentStall {
		t.Fatalf("observation-only continuation = verdict=%s code=%q", verdict, code)
	}
}

func TestAgentOutcomeGuardAllowsProductiveContinuationAfterObservationHistory(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "metadata":{"agent_contract":{"requires":["mutation"]}},
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"read-1","type":"function","function":{"name":"Read","arguments":"{}"}}]},
    {"role":"tool","tool_call_id":"read-1","content":"ok"},
    {"role":"assistant","tool_calls":[{"id":"read-2","type":"function","function":{"name":"Read","arguments":"{}"}}]},
    {"role":"tool","tool_call_id":"read-2","content":"ok"}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolChat, 3)
	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"write-3","type":"function","function":{"name":"Write","arguments":"{\"path\":\"out.txt\",\"content\":\"ok\"}"}}]},"finish_reason":"tool_calls"}]}`,
		))),
		qualityProtocolChat,
		QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond, AgentStallTurns: 3},
		requirement,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = replay.Close()
	if verdict != QualityDeliver || code != "" {
		t.Fatalf("productive continuation = verdict=%s code=%q", verdict, code)
	}
}

func TestAgentOutcomeGuardLeavesRequestsWithoutContractUntouched(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"read-1","type":"function","function":{"name":"Read","arguments":"{}"}}]},
    {"role":"tool","tool_call_id":"read-1","content":"ok"}
  ]
}`)
	if requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolChat, 2); requirement.Enabled {
		t.Fatalf("request without explicit contract enabled guard: %#v", requirement)
	}
}

func TestAgentOutcomeGuardRejectsEmptyTerminalAfterImplicitObservationTail(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "messages":[
    {"role":"assistant","content":[{"type":"tool_use","id":"read-1","name":"Read","input":{"file_path":"a.txt"}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"read-1","is_error":false,"content":"ok"}]},
    {"role":"assistant","content":[{"type":"tool_use","id":"read-2","name":"Read","input":{"file_path":"b.txt"}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"read-2","is_error":false,"content":"ok"}]}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolAnthropic, 3)
	if !requirement.Enabled || !requirement.RejectEmptyTerminal || requirement.ObservationActions != 2 {
		t.Fatalf("implicit empty-terminal requirement = %#v", requirement)
	}

	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking"}}`,
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"checking"}}`,
			`data: {"type":"message_stop"}`,
		))),
		qualityProtocolAnthropic,
		QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond, AgentStallTurns: 3},
		requirement,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = replay.Close()
	if verdict != QualityWithhold || code != ErrorAgentStall {
		t.Fatalf("empty terminal = verdict=%s code=%q", verdict, code)
	}
}

func TestAgentOutcomeGuardAllowsVisibleTerminalAfterImplicitObservationTail(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "messages":[
    {"role":"assistant","content":[{"type":"tool_use","id":"read-1","name":"Read","input":{"file_path":"a.txt"}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"read-1","is_error":false,"content":"ok"}]},
    {"role":"assistant","content":[{"type":"tool_use","id":"read-2","name":"Read","input":{"file_path":"b.txt"}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"read-2","is_error":false,"content":"ok"}]}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolAnthropic, 3)
	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"text"}}`,
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Review complete."}}`,
			`data: {"type":"message_stop"}`,
		))),
		qualityProtocolAnthropic,
		QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond, AgentStallTurns: 3},
		requirement,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = replay.Close()
	if verdict != QualityDeliver || code != "" {
		t.Fatalf("visible terminal = verdict=%s code=%q", verdict, code)
	}
}

func TestObserveQualityResponsesMarksCodexToolCapabilities(t *testing.T) {
	t.Parallel()
	state := qualityScanState{protocol: qualityProtocolResponses}
	ObserveQualityChunk(&state, []byte(sse(
		`data: {"type":"response.output_item.added","item":{"id":"call_1","type":"function_call","name":"shell_command","arguments":""}}`,
		`data: {"type":"response.function_call_arguments.done","item_id":"call_1","arguments":"{\"command\":\"go test ./...\"}"}`,
		`data: {"type":"response.output_item.done","item":{"id":"call_1","type":"function_call","name":"shell_command","arguments":"{\"command\":\"go test ./...\"}"}}`,
	)))
	if !state.hasOutputToolUse || !state.hasOutputExecution || !state.hasOutputVerification {
		t.Fatalf("Codex tool capability state = %#v", state)
	}
}
