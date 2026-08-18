package gateway

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

func TestAgentOutcomeStrictReceiptsAcrossProtocols(t *testing.T) {
	t.Parallel()
	const receipt = `{"receipt":{"exit_code":0,"artifact":{"exists":true},"svg":{"valid":true,"references_valid":true},"browser":{"assertions_passed":true}}}`
	tests := []struct {
		name     string
		protocol string
		body     []byte
		fixture  string
	}{
		{
			name:     "Responses",
			protocol: qualityProtocolResponses,
			body: []byte(`{
  "metadata":{"agent_contract":{"requires":["command_exit_zero","artifact_exists","svg_valid","browser_assertions_passed"]}},
  "input":[
    {"type":"function_call","call_id":"outcome-1","name":"shell_command","arguments":"{\"command\":\"go test ./...\"}"},
    {"type":"function_call_output","call_id":"outcome-1","status":"completed","output":` + receipt + `}
  ]
}`),
			fixture: sse(
				`data: {"type":"response.reasoning_text.delta","delta":"checking result"}`,
				`data: {"type":"response.output_text.delta","delta":"done"}`,
				`data: {"type":"response.completed"}`,
			),
		},
		{
			name:     "Chat",
			protocol: qualityProtocolChat,
			body: []byte(`{
  "metadata":{"agent_contract":{"requires":["command_exit_zero","artifact_exists","svg_valid","browser_assertions_passed"]}},
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"outcome-1","type":"function","function":{"name":"shell_command","arguments":"{\"command\":\"go test ./...\"}"}}]},
    {"role":"tool","tool_call_id":"outcome-1","content":"` + strings.ReplaceAll(receipt, `"`, `\"`) + `"}
  ]
}`),
			fixture: sse(
				`data: {"choices":[{"delta":{"thinking_content":"checking result"}}]}`,
				`data: {"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}`,
			),
		},
		{
			name:     "Anthropic",
			protocol: qualityProtocolAnthropic,
			body: []byte(`{
  "metadata":{"agent_contract":{"requires":["command_exit_zero","artifact_exists","svg_valid","browser_assertions_passed"]}},
  "messages":[
    {"role":"assistant","content":[{"type":"tool_use","id":"outcome-1","name":"Bash","input":{"command":"go test ./..."}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"outcome-1","is_error":false,"content":[{"type":"text","text":"` + strings.ReplaceAll(receipt, `"`, `\"`) + `"}]}]}
  ]
}`),
			fixture: sse(
				`data: {"type":"content_block_start","content_block":{"type":"thinking"}}`,
				`data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"checking result"}}`,
				`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"done"}}`,
				`data: {"type":"message_stop"}`,
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requirement := agentOutcomeRequirementFromRequest(test.body, test.protocol, 6)
			if !requirement.Enabled || !requirement.VerifiedCommandExitZero || !requirement.VerifiedArtifactExists || !requirement.VerifiedSVGValid || !requirement.VerifiedBrowserAssertionsPassed {
				t.Fatalf("strict receipt requirement = %#v", requirement)
			}
			if !requirement.contractSatisfied() {
				t.Fatalf("strict receipt contract unexpectedly unfulfilled: %#v", requirement)
			}

			replay, verdict, code, _, _, err := peekQualityStream(
				context.Background(),
				io.NopCloser(strings.NewReader(test.fixture)),
				test.protocol,
				QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond},
				requirement,
			)
			if err != nil {
				t.Fatal(err)
			}
			_ = replay.Close()
			if verdict != QualityDeliver || code != "" {
				t.Fatalf("strict receipt outcome = verdict=%s code=%q", verdict, code)
			}
		})
	}
}

func TestAgentOutcomeStrictReceiptRejectsPlainFailedAndUnlinkedResults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
	}{
		{
			name: "plain tool text is not receipt evidence",
			body: `{
  "metadata":{"agent_contract":{"requires":["artifact_exists"]}},
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"write-1","type":"function","function":{"name":"Write","arguments":"{}"}}]},
    {"role":"tool","tool_call_id":"write-1","content":"File written successfully"}
  ]
}`,
		},
		{
			name: "unlinked receipt cannot satisfy contract",
			body: `{
  "metadata":{"agent_contract":{"requires":["artifact_exists"]}},
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"write-1","type":"function","function":{"name":"Write","arguments":"{}"}}]},
    {"role":"tool","tool_call_id":"other-call","content":"{\"artifact\":{\"exists\":true}}"}
  ]
}`,
		},
		{
			name: "nonzero exit code cannot satisfy command receipt",
			body: `{
  "metadata":{"agent_contract":{"requires":["command_exit_zero"]}},
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"test-1","type":"function","function":{"name":"shell_command","arguments":"{\"command\":\"go test ./...\"}"}}]},
    {"role":"tool","tool_call_id":"test-1","content":"{\"exit_code\":1}"}
  ]
}`,
		},
		{
			name: "SVG receipt requires reference validation",
			body: `{
  "metadata":{"agent_contract":{"requires":["svg_valid"]}},
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"svg-1","type":"function","function":{"name":"shell_command","arguments":"{\"command\":\"node validate-svg.mjs out.svg\"}"}}]},
    {"role":"tool","tool_call_id":"svg-1","content":"{\"svg\":{\"valid\":true,\"references_valid\":false}}"}
  ]
}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requirement := agentOutcomeRequirementFromRequest([]byte(test.body), qualityProtocolChat, 6)
			if !requirement.Enabled || requirement.contractSatisfied() {
				t.Fatalf("invalid receipt unexpectedly satisfied contract: %#v", requirement)
			}
			replay, verdict, code, _, _, err := peekQualityStream(
				context.Background(),
				io.NopCloser(strings.NewReader(sse(
					`data: {"choices":[{"delta":{"thinking_content":"checking"}}]}`,
					`data: {"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}`,
				))),
				qualityProtocolChat,
				QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond},
				requirement,
			)
			if err != nil {
				t.Fatal(err)
			}
			_ = replay.Close()
			if verdict != QualityWithhold || code != ErrorAgentOutcomeUnverified {
				t.Fatalf("invalid strict receipt = verdict=%s code=%q", verdict, code)
			}
		})
	}
}

func TestAgentOutcomeEmbeddedReceiptEnvelopeSurvivesMetadataNormalization(t *testing.T) {
	t.Parallel()
	const envelope = `{"type":"grok_agent_outcome_receipt","version":1,"requires":["artifact_exists","svg_valid"],"receipt":{"artifact":{"exists":true},"svg":{"valid":true,"references_valid":true}}}`
	body := func(receipt string) []byte {
		return []byte(`{
  "messages":[
    {"role":"assistant","content":[{"type":"tool_use","id":"toolu_embedded","name":"Write","input":{"file_path":"out.svg"}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_embedded","is_error":false,"content":[{"type":"text","text":"` + strings.ReplaceAll(receipt, `"`, `\"`) + `"}]}]}
  ]
}`)
	}

	valid := agentOutcomeRequirementFromRequest(body(envelope), qualityProtocolAnthropic, 6)
	if !valid.Enabled || !valid.ContractOpen || !valid.contractSatisfied() {
		t.Fatalf("embedded valid receipt requirement = %#v", valid)
	}
	if required := valid.RequiredToolReceipts["toolu_embedded"]; !required.has(agentOutcomeReceiptArtifactExists) || !required.has(agentOutcomeReceiptSVGValid) {
		t.Fatalf("embedded requirement was not bound to tool call: %#v", valid.RequiredToolReceipts)
	}

	broken := strings.Replace(envelope, `"references_valid":true`, `"references_valid":false`, 1)
	invalid := agentOutcomeRequirementFromRequest(body(broken), qualityProtocolAnthropic, 6)
	if !invalid.Enabled || invalid.contractSatisfied() {
		t.Fatalf("broken embedded receipt unexpectedly satisfied contract: %#v", invalid)
	}
	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"type":"content_block_start","content_block":{"type":"thinking"}}`,
			`data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"done"}}`,
			`data: {"type":"message_stop"}`,
		))),
		qualityProtocolAnthropic,
		QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond},
		invalid,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = replay.Close()
	if verdict != QualityWithhold || code != ErrorAgentOutcomeUnverified {
		t.Fatalf("broken embedded receipt = verdict=%s code=%q", verdict, code)
	}

	plain := agentOutcomeRequirementFromRequest(body("grok_agent_outcome_receipt artifact_exists svg_valid"), qualityProtocolAnthropic, 6)
	if plain.Enabled {
		t.Fatalf("plain text unexpectedly opened an embedded receipt contract: %#v", plain)
	}
}

func TestAgentOutcomeStrictReceiptAllowsActionBeforeResult(t *testing.T) {
	t.Parallel()
	requirement := agentOutcomeRequirementFromRequest([]byte(`{
  "metadata":{"agent_contract":{"requires":["artifact_exists","svg_valid"]}},
  "messages":[{"role":"user","content":"create the requested SVG"}]
}`), qualityProtocolAnthropic, 6)
	if !requirement.Enabled || requirement.contractSatisfied() {
		t.Fatalf("strict action requirement = %#v", requirement)
	}

	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"type":"content_block_start","index":0,"content_block":{"id":"toolu_1","type":"tool_use","name":"Bash","input":{}}}`,
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"command\":\"node validate-svg.mjs out.svg\"}"}}`,
			`data: {"type":"message_stop"}`,
		))),
		qualityProtocolAnthropic,
		QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond},
		requirement,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = replay.Close()
	if verdict != QualityDeliver || code != "" {
		t.Fatalf("outgoing tool action = verdict=%s code=%q", verdict, code)
	}
}

func TestAgentOutcomeToolReceiptsRequireEachBoundCall(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "metadata":{"agent_contract":{"tool_receipts":[
    {"tool_call_id":"write-1","requires":["artifact_exists"]},
    {"tool_call_id":"svg-1","requires":["svg_valid"]}
  ]}},
  "messages":[
    {"role":"assistant","tool_calls":[
      {"id":"write-1","type":"function","function":{"name":"Write","arguments":"{\"file_path\":\"out.txt\"}"}},
      {"id":"svg-1","type":"function","function":{"name":"Write","arguments":"{\"file_path\":\"out.svg\"}"}}
    ]},
    {"role":"tool","tool_call_id":"write-1","content":"{\"receipt\":{\"artifact\":{\"exists\":true}}}"},
    {"role":"tool","tool_call_id":"svg-1","content":"{\"receipt\":{\"svg\":{\"valid\":true,\"references_valid\":false}}}"}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolChat, 6)
	if !requirement.Enabled || len(requirement.RequiredToolReceipts) != 2 {
		t.Fatalf("per-call requirement = %#v", requirement)
	}
	if !requirement.VerifiedToolReceipts["write-1"].has(agentOutcomeReceiptArtifactExists) {
		t.Fatalf("write receipt missing: %#v", requirement.VerifiedToolReceipts)
	}
	if requirement.VerifiedToolReceipts["svg-1"].has(agentOutcomeReceiptSVGValid) {
		t.Fatalf("invalid SVG receipt unexpectedly verified: %#v", requirement.VerifiedToolReceipts)
	}
	if requirement.contractSatisfied() {
		t.Fatalf("one successful artifact must not satisfy all bound receipts: %#v", requirement)
	}

	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"choices":[{"delta":{"thinking_content":"checking"}}]}`,
			`data: {"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}`,
		))),
		qualityProtocolChat,
		QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond},
		requirement,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = replay.Close()
	if verdict != QualityWithhold || code != ErrorAgentOutcomeUnverified {
		t.Fatalf("per-call outcome = verdict=%s code=%q", verdict, code)
	}

	valid := agentOutcomeRequirementFromRequest(
		[]byte(strings.Replace(string(body), `\"references_valid\":false`, `\"references_valid\":true`, 1)),
		qualityProtocolChat,
		6,
	)
	if !valid.contractSatisfied() {
		t.Fatalf("valid per-call receipts unexpectedly unfulfilled: %#v", valid)
	}
}

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

func TestAgentOutcomeGuardRecoversFailedVerificationWithoutContract(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"test-1","type":"function","function":{"name":"Bash","arguments":"{\"command\":\"npm test\"}"}}]},
    {"role":"tool","tool_call_id":"test-1","content":"{\"exit_code\":1}"}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolChat, 6)
	if !requirement.Enabled || !requirement.ContractOpen || requirement.RequiresMutation || !requirement.RequiresExecution || !requirement.RequiresVerification {
		t.Fatalf("implicit failed-verification recovery = %#v", requirement)
	}

	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"choices":[{"delta":{"content":"Tests pass now."},"finish_reason":"stop"}]}`,
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
		t.Fatalf("implicit failed-verification recovery = verdict=%s code=%q", verdict, code)
	}
}

func TestAgentOutcomeGuardAllowsSuccessfulVerificationRecoveryWithoutContract(t *testing.T) {
	t.Parallel()
	body := []byte(`{
  "messages":[
    {"role":"assistant","tool_calls":[{"id":"test-1","type":"function","function":{"name":"Bash","arguments":"{\"command\":\"npm test\"}"}}]},
    {"role":"tool","tool_call_id":"test-1","content":"{\"exit_code\":1}"},
    {"role":"assistant","tool_calls":[{"id":"test-2","type":"function","function":{"name":"Bash","arguments":"{\"command\":\"npm test\"}"}}]},
    {"role":"tool","tool_call_id":"test-2","content":"{\"exit_code\":0}"}
  ]
}`)
	requirement := agentOutcomeRequirementFromRequest(body, qualityProtocolChat, 6)
	if !requirement.contractSatisfied() || !requirement.VerifiedExecution || !requirement.VerifiedVerification {
		t.Fatalf("successful verification recovery = %#v", requirement)
	}

	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(sse(
			`data: {"choices":[{"delta":{"content":"Tests pass now."},"finish_reason":"stop"}]}`,
		))),
		qualityProtocolChat,
		QualityRetryRuntime{Enabled: true, AgentOutcomeGuard: true, HoldTimeout: time.Millisecond},
		requirement,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = replay.Close()
	if verdict != QualityDeliver || code != "" {
		t.Fatalf("successful verification recovery = verdict=%s code=%q", verdict, code)
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
