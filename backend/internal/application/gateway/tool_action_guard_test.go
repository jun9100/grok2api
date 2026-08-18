package gateway

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestToolActionRequirementFromRequest(t *testing.T) {
	t.Parallel()
	failedWrite := []byte(`{
  "tools":[{"name":"Read"},{"name":"Write"},{"name":"Bash"}],
  "messages":[
    {"role":"user","content":"用svg画一个骑自行车的鹈鹕动图并保存为文件"},
    {"role":"assistant","content":[{"type":"tool_use","id":"write-1","name":"Write","input":{"file_path":"/Users/ron/pelican-bicycle.svg","content":"<svg/>"}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"write-1","is_error":true,"content":"File has not been read yet"}]}
  ]
}`)
	requirement := toolActionRequirementFromRequest(failedWrite)
	if !requirement.Enabled || !requirement.ArtifactRequested || !requirement.UnresolvedWriteFailure || requirement.VerifiedWrite {
		t.Fatalf("failed Write requirement = %#v", requirement)
	}

	verifiedWrite := []byte(`{
  "tools":[{"name":"Write"}],
  "messages":[
    {"role":"user","content":"创建一个 svg 文件"},
    {"role":"assistant","content":[{"type":"tool_use","id":"write-1","name":"Write","input":{"file_path":"/tmp/a.svg","content":"<svg/>"}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"write-1","is_error":false,"content":"File written successfully"}]}
  ]
}`)
	requirement = toolActionRequirementFromRequest(verifiedWrite)
	if !requirement.Enabled || !requirement.ArtifactRequested || requirement.UnresolvedWriteFailure || !requirement.VerifiedWrite {
		t.Fatalf("verified Write requirement = %#v", requirement)
	}

	plainQuestion := []byte(`{
  "tools":[{"name":"Write"}],
  "messages":[{"role":"user","content":"解释 SVG 动画的工作原理"}]
}`)
	if got := toolActionRequirementFromRequest(plainQuestion); got.Enabled {
		t.Fatalf("plain question unexpectedly enabled strict guard: %#v", got)
	}
}

func TestPeekQualityStreamWithholdsManualArtifactDeferralDespiteThinking(t *testing.T) {
	t.Parallel()
	fixture := sse(
		`data: {"type":"content_block_start","content_block":{"type":"thinking"}}`,
		`data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"I should create the requested artifact."}}`,
		`data: {"type":"content_block_start","content_block":{"type":"text"}}`,
		`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"<svg></svg>"}}`,
		`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"复制上面代码并保存为 pelican-bicycle.svg。"}}`,
		`data: {"type":"message_stop"}`,
	)
	replay, verdict, code, _, _, err := peekQualityStream(
		context.Background(),
		io.NopCloser(strings.NewReader(fixture)),
		qualityProtocolAnthropic,
		QualityRetryRuntime{Enabled: true, ToolActionGuard: true, HoldTimeout: time.Millisecond},
		toolActionRequirement{Enabled: true, ArtifactRequested: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if verdict != QualityWithhold || code != ErrorToolActionUnverified {
		t.Fatalf("manual artifact deferral = verdict=%s code=%q", verdict, code)
	}
	if body, _ := io.ReadAll(replay); !strings.Contains(string(body), "pelican-bicycle.svg") {
		t.Fatalf("held body lost terminal artifact text: %s", body)
	}
}

func TestPeekQualityStreamToolActionEvidence(t *testing.T) {
	t.Parallel()
	claim := sse(
		`data: {"type":"content_block_start","content_block":{"type":"thinking"}}`,
		`data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"verify the write"}}`,
		`data: {"type":"content_block_start","content_block":{"type":"text"}}`,
		`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"已经写好了 /tmp/pelican.svg"}}`,
		`data: {"type":"message_stop"}`,
	)
	tests := []struct {
		name        string
		requirement toolActionRequirement
		fixture     string
		wantVerdict QualityVerdict
		wantCode    string
	}{
		{
			name:        "unresolved Write cannot be claimed complete",
			requirement: toolActionRequirement{Enabled: true, ArtifactRequested: true, UnresolvedWriteFailure: true},
			fixture:     claim,
			wantVerdict: QualityWithhold,
			wantCode:    ErrorToolActionUnverified,
		},
		{
			name:        "verified Write permits completion claim",
			requirement: toolActionRequirement{Enabled: true, ArtifactRequested: true, VerifiedWrite: true},
			fixture:     claim,
			wantVerdict: QualityDeliver,
		},
		{
			name:        "outgoing tool use remains actionable",
			requirement: toolActionRequirement{Enabled: true, ArtifactRequested: true},
			fixture: sse(
				`data: {"type":"content_block_start","content_block":{"type":"thinking"}}`,
				`data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"write it"}}`,
				`data: {"type":"content_block_start","content_block":{"type":"tool_use","name":"Write"}}`,
				`data: {"type":"content_block_delta","delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"/tmp/pelican.svg\"}"}}`,
				`data: {"type":"message_stop"}`,
			),
			wantVerdict: QualityDeliver,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			replay, verdict, code, _, _, err := peekQualityStream(
				context.Background(), io.NopCloser(strings.NewReader(test.fixture)), qualityProtocolAnthropic,
				QualityRetryRuntime{Enabled: true, ToolActionGuard: true, HoldTimeout: time.Millisecond}, test.requirement,
			)
			if err != nil {
				t.Fatal(err)
			}
			_ = replay.Close()
			if verdict != test.wantVerdict || code != test.wantCode {
				t.Fatalf("outcome = verdict=%s code=%q, want verdict=%s code=%q", verdict, code, test.wantVerdict, test.wantCode)
			}
		})
	}
}
