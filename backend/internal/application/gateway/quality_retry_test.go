package gateway

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	accountapp "github.com/chenyme/grok2api/backend/internal/application/account"
	clientkeyapp "github.com/chenyme/grok2api/backend/internal/application/clientkey"
	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/domain/audit"
	"github.com/chenyme/grok2api/backend/internal/domain/clientkey"
	inferencedomain "github.com/chenyme/grok2api/backend/internal/domain/inference"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/runtime/memory"
)

func TestClassifyQualityHold(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sig  QualityStreamSignals
		want QualityVerdict
	}{
		{name: "thinking delivers", sig: QualityStreamSignals{HasThinking: true, VisibleTokens: 10}, want: QualityDeliver},
		{name: "reasoning tokens deliver", sig: QualityStreamSignals{ReasoningTokens: 40, VisibleTokens: 80, Terminal: true}, want: QualityDeliver},
		{name: "tool use delivers without thinking", sig: QualityStreamSignals{HasToolUse: true, OutputTokens: 40, Terminal: true}, want: QualityDeliver},
		{name: "visible 32 no think withhold", sig: QualityStreamSignals{VisibleTokens: 32, Terminal: true}, want: QualityWithhold},
		{name: "output 40 no think withhold", sig: QualityStreamSignals{OutputTokens: 40, Terminal: true}, want: QualityWithhold},
		{name: "short no think delivers", sig: QualityStreamSignals{VisibleTokens: 10, Terminal: true}, want: QualityDeliver},
		{name: "midstream enough content withhold", sig: QualityStreamSignals{VisibleTokens: 64}, want: QualityWithhold},
		{name: "wait for more", sig: QualityStreamSignals{VisibleTokens: 8}, want: QualityWait},
		{name: "hold expired short delivers", sig: QualityStreamSignals{VisibleTokens: 8, HoldExpired: true}, want: QualityDeliver},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyQualityHold(test.sig, 32); got != test.want {
				t.Fatalf("ClassifyQualityHold() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestDecideQualityRetry(t *testing.T) {
	t.Parallel()
	if got := DecideQualityRetry(QualityDeliver, 0, 2, qualityRetryFailOpen); got != QualityActionDeliver {
		t.Fatalf("deliver verdict: %s", got)
	}
	if got := DecideQualityRetry(QualityWithhold, 0, 2, qualityRetryFailOpen); got != QualityActionRetry {
		t.Fatalf("first withhold: %s", got)
	}
	if got := DecideQualityRetry(QualityWithhold, 1, 2, qualityRetryFailOpen); got != QualityActionDeliverLast {
		t.Fatalf("last fail-open: %s", got)
	}
	if got := DecideQualityRetry(QualityWithhold, 1, 2, qualityRetryFailClosed); got != QualityActionReject {
		t.Fatalf("last fail-closed: %s", got)
	}
	if got := DecideQualityRetry(QualityWithhold, 0, 1, qualityRetryFailOpen); got != QualityActionDeliverLast {
		t.Fatalf("max 1 fail-open: %s", got)
	}
}

func TestDecideQualityRetryLastWithholdIsMaxAttemptsMinusOne(t *testing.T) {
	t.Parallel()
	for _, maxAttempts := range []int{1, 2, 3, 6} {
		last := maxAttempts - 1
		if got := DecideQualityRetry(QualityWithhold, last, maxAttempts, qualityRetryFailOpen); got != QualityActionDeliverLast {
			t.Fatalf("fail-open last withhold max=%d index=%d got %s", maxAttempts, last, got)
		}
		if got := DecideQualityRetry(QualityWithhold, last, maxAttempts, qualityRetryFailClosed); got != QualityActionReject {
			t.Fatalf("fail-closed last withhold max=%d index=%d got %s", maxAttempts, last, got)
		}
		if last > 0 {
			if got := DecideQualityRetry(QualityWithhold, last-1, maxAttempts, qualityRetryFailOpen); got != QualityActionRetry {
				t.Fatalf("pre-last should retry max=%d index=%d got %s", maxAttempts, last-1, got)
			}
		}
	}
}

func TestCommitQualityHold(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		verdict        QualityVerdict
		qualityAttempt int
		maxAttempts    int
		hasNext        bool
		onExhausted    string
		wantAction     QualityRetryAction
		wantAudit      bool
		wantKeep       bool
	}{
		{
			name:    "first withhold + hasNext → Retry+Audit",
			verdict: QualityWithhold, qualityAttempt: 0, maxAttempts: 2, hasNext: true, onExhausted: qualityRetryFailOpen,
			wantAction: QualityActionRetry, wantAudit: true, wantKeep: false,
		},
		{
			name:    "last withhold fail-open → DeliverLast+KeepBody",
			verdict: QualityWithhold, qualityAttempt: 1, maxAttempts: 2, hasNext: true, onExhausted: qualityRetryFailOpen,
			wantAction: QualityActionDeliverLast, wantAudit: true, wantKeep: true,
		},
		{
			name:    "last withhold fail-closed → Reject+Audit",
			verdict: QualityWithhold, qualityAttempt: 1, maxAttempts: 2, hasNext: true, onExhausted: qualityRetryFailClosed,
			wantAction: QualityActionReject, wantAudit: true, wantKeep: false,
		},
		{
			name:    "routing exhausted even at qualityAttempt=0 → not Retry",
			verdict: QualityWithhold, qualityAttempt: 0, maxAttempts: 2, hasNext: false, onExhausted: qualityRetryFailOpen,
			wantAction: QualityActionDeliverLast, wantAudit: true, wantKeep: true,
		},
		{
			name:    "thinking delivers keep body",
			verdict: QualityDeliver, qualityAttempt: 0, maxAttempts: 2, hasNext: true, onExhausted: qualityRetryFailOpen,
			wantAction: QualityActionDeliver, wantAudit: false, wantKeep: true,
		},
		{
			name:    "switch 5 times: attempt 4 of 6 still retries",
			verdict: QualityWithhold, qualityAttempt: 4, maxAttempts: 6, hasNext: true, onExhausted: qualityRetryFailClosed,
			wantAction: QualityActionRetry, wantAudit: true, wantKeep: false,
		},
		{
			name:    "switch 5 times: attempt 5 of 6 fail-closed rejects no body",
			verdict: QualityWithhold, qualityAttempt: 5, maxAttempts: 6, hasNext: true, onExhausted: qualityRetryFailClosed,
			wantAction: QualityActionReject, wantAudit: true, wantKeep: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := CommitQualityHold(test.verdict, test.qualityAttempt, test.maxAttempts, test.hasNext, test.onExhausted)
			if got.Action != test.wantAction || got.Audit != test.wantAudit || got.KeepBody != test.wantKeep {
				t.Fatalf("CommitQualityHold() = %+v, want action=%s audit=%t keep=%t", got, test.wantAction, test.wantAudit, test.wantKeep)
			}
			if got.Action == QualityActionRetry && !test.hasNext {
				t.Fatal("routing exhausted must not Retry")
			}
		})
	}
}

func TestBoundQualityRetryWhenRoutingExhausted(t *testing.T) {
	t.Parallel()
	if got := BoundQualityRetry(QualityActionRetry, true, qualityRetryFailOpen); got != QualityActionRetry {
		t.Fatalf("has next: %s", got)
	}
	if got := BoundQualityRetry(QualityActionRetry, false, qualityRetryFailOpen); got != QualityActionDeliverLast {
		t.Fatalf("no next fail-open: %s", got)
	}
	if got := BoundQualityRetry(QualityActionRetry, false, qualityRetryFailClosed); got != QualityActionReject {
		t.Fatalf("no next fail-closed: %s", got)
	}
	if got := BoundQualityRetry(QualityActionDeliverLast, false, qualityRetryFailOpen); got != QualityActionDeliverLast {
		t.Fatalf("already last: %s", got)
	}
}

func sse(frames ...string) string {
	var b strings.Builder
	for _, frame := range frames {
		b.WriteString(frame)
		if !strings.HasSuffix(frame, "\n") {
			b.WriteByte('\n')
		}
		if !strings.HasSuffix(frame, "\n\n") {
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func TestObserveQualityChunkThinkingChat(t *testing.T) {
	t.Parallel()
	state := qualityScanState{protocol: qualityProtocolChat}
	ObserveQualityChunk(&state, []byte(sse(
		": grok2api-reasoning-start",
		`data: {"choices":[{"delta":{"thinking_content":"plan the game"}}]}`,
		`data: {"choices":[{"delta":{"content":"here is a game"}}]}`,
		`data: {"usage":{"completion_tokens":80,"completion_tokens_details":{"reasoning_tokens":40}}}`,
		"data: [DONE]",
	)))
	sig := state.signals()
	if !sig.HasThinking || !sig.Terminal || sig.ReasoningTokens != 40 {
		t.Fatalf("thinking fixture signals = %#v", sig)
	}
	if ClassifyQualityHold(sig, 32) != QualityDeliver {
		t.Fatalf("thinking fixture withheld")
	}
}

func TestObserveQualityChunkNoThinkEnoughChat(t *testing.T) {
	t.Parallel()
	state := qualityScanState{protocol: qualityProtocolChat}
	content := strings.Repeat("word ", 40) // 200 runes → 50 tokens
	ObserveQualityChunk(&state, []byte(sse(
		`data: {"choices":[{"delta":{"content":"`+content+`"}}]}`,
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
		`data: {"usage":{"completion_tokens":50,"completion_tokens_details":{"reasoning_tokens":0}}}`,
		"data: [DONE]",
	)))
	sig := state.signals()
	if sig.HasThinking || !sig.Terminal || sig.VisibleTokens < 32 {
		t.Fatalf("no-think fixture signals = %#v", sig)
	}
	if ClassifyQualityHold(sig, 32) != QualityWithhold {
		t.Fatalf("no-think enough should withhold, got %s (%#v)", ClassifyQualityHold(sig, 32), sig)
	}
}

func TestObserveQualityChunkNoThinkAnthropicToolUseDelivers(t *testing.T) {
	t.Parallel()
	state := qualityScanState{protocol: qualityProtocolAnthropic}
	ObserveQualityChunk(&state, []byte(sse(
		`data: {"type":"content_block_start","index":0,"content_block":{"id":"toolu_1","type":"tool_use","name":"Bash","input":{}}}`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"command\":\"test -s review.md\"}"}}`,
		`data: {"type":"message_delta","usage":{"output_tokens":40,"output_tokens_details":{"thinking_tokens":0}}}`,
		`data: {"type":"message_stop"}`,
	)))
	sig := state.signals()
	if !sig.HasToolUse || sig.HasThinking || !sig.Terminal {
		t.Fatalf("tool-use fixture signals = %#v", sig)
	}
	if ClassifyQualityHold(sig, 32) != QualityDeliver {
		t.Fatalf("no-think tool-use should deliver, got %s (%#v)", ClassifyQualityHold(sig, 32), sig)
	}
}

func TestObserveQualityChunkShortNoThink(t *testing.T) {
	t.Parallel()
	state := qualityScanState{protocol: qualityProtocolChat}
	ObserveQualityChunk(&state, []byte(sse(
		`data: {"choices":[{"delta":{"content":"ok"}}]}`,
		"data: [DONE]",
	)))
	sig := state.signals()
	if ClassifyQualityHold(sig, 32) != QualityDeliver {
		t.Fatalf("short no-think should deliver, got %s (%#v)", ClassifyQualityHold(sig, 32), sig)
	}
}

func TestObserveQualityChunkResponsesReasoningItem(t *testing.T) {
	t.Parallel()
	state := qualityScanState{protocol: qualityProtocolResponses}
	ObserveQualityChunk(&state, []byte(sse(
		`data: {"type":"response.output_item.added","item":{"id":"rs_1","type":"reasoning"}}`,
		`data: {"type":"response.output_text.delta","delta":"hello"}`,
		`data: {"type":"response.completed","response":{"id":"resp_1","usage":{"output_tokens":90,"output_tokens_details":{"reasoning_tokens":60}}}}`,
	)))
	if ClassifyQualityHold(state.signals(), 32) != QualityDeliver {
		t.Fatalf("responses reasoning item should deliver: %#v", state.signals())
	}
}

func TestPeekQualityStreamThinkingDeliversRemainder(t *testing.T) {
	t.Parallel()
	body := io.NopCloser(strings.NewReader(sse(
		`data: {"choices":[{"delta":{"thinking_content":"think"}}]}`,
		`data: {"choices":[{"delta":{"content":"answer after think"}}]}`,
		"data: [DONE]",
	)))
	replay, verdict, _, _, _, err := peekQualityStream(context.Background(), body, qualityProtocolChat, QualityRetryRuntime{MinOutputTokens: 32, HoldTimeout: time.Second}, toolActionRequirement{})
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if verdict != QualityDeliver {
		t.Fatalf("verdict=%s", verdict)
	}
	got, _ := io.ReadAll(replay)
	if !strings.Contains(string(got), "answer after think") || !strings.Contains(string(got), "thinking_content") {
		t.Fatalf("replay lost frames: %s", got)
	}
}

func TestPeekQualityStreamWithholdsNoThinkEnough(t *testing.T) {
	t.Parallel()
	content := strings.Repeat("abcd", 16) // 64 runes → 16 tokens... need 32 tokens = 128 runes
	content = strings.Repeat("abcd", 40)  // 160 runes → 40 tokens
	body := io.NopCloser(strings.NewReader(sse(
		`data: {"choices":[{"delta":{"content":"`+content+`"}}]}`,
		`data: {"usage":{"completion_tokens":40,"completion_tokens_details":{"reasoning_tokens":0}}}`,
		"data: [DONE]",
	)))
	replay, verdict, _, usage, _, err := peekQualityStream(context.Background(), body, qualityProtocolChat, QualityRetryRuntime{MinOutputTokens: 32, HoldTimeout: time.Second}, toolActionRequirement{})
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if verdict != QualityWithhold {
		t.Fatalf("verdict=%s usage=%#v", verdict, usage)
	}
	if usage.ReasoningTokens != 0 || usage.OutputTokens < 32 {
		t.Fatalf("usage=%#v", usage)
	}
}

func TestPeekQualityStreamEstimatesHeldVisibleOutputWithoutUsageFrame(t *testing.T) {
	t.Parallel()
	content := strings.Repeat("abcd", 40)
	body := io.NopCloser(strings.NewReader(sse(
		`data: {"choices":[{"delta":{"content":"`+content+`"}}]}`,
		"data: [DONE]",
	)))
	replay, verdict, _, usage, _, err := peekQualityStream(context.Background(), body, qualityProtocolChat, QualityRetryRuntime{MinOutputTokens: 32, HoldTimeout: time.Second}, toolActionRequirement{})
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if verdict != QualityWithhold || usage.OutputTokens < 32 || usage.ReasoningTokens != 0 {
		t.Fatalf("verdict=%s usage=%#v", verdict, usage)
	}
}

func TestPeekThenDecideQualityRetryBounded(t *testing.T) {
	t.Parallel()
	content := strings.Repeat("abcd", 40)
	fixture := sse(
		`data: {"choices":[{"delta":{"content":"`+content+`"}}]}`,
		`data: {"usage":{"completion_tokens":40,"completion_tokens_details":{"reasoning_tokens":0}}}`,
		"data: [DONE]",
	)
	cfg := QualityRetryRuntime{MinOutputTokens: 32, MaxAttempts: 2, OnExhausted: qualityRetryFailOpen, HoldTimeout: time.Second}

	replay, verdict, _, usage, _, err := peekQualityStream(context.Background(), io.NopCloser(strings.NewReader(fixture)), qualityProtocolChat, cfg, toolActionRequirement{})
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if verdict != QualityWithhold {
		t.Fatalf("first peek verdict=%s usage=%#v", verdict, usage)
	}
	if got := DecideQualityRetry(verdict, 0, cfg.MaxAttempts, cfg.OnExhausted); got != QualityActionRetry {
		t.Fatalf("first withhold action=%s", got)
	}

	replay2, verdict2, _, _, _, err := peekQualityStream(context.Background(), io.NopCloser(strings.NewReader(fixture)), qualityProtocolChat, cfg, toolActionRequirement{})
	if err != nil {
		t.Fatal(err)
	}
	defer replay2.Close()
	if verdict2 != QualityWithhold {
		t.Fatalf("second peek verdict=%s", verdict2)
	}
	action2 := DecideQualityRetry(verdict2, 1, cfg.MaxAttempts, cfg.OnExhausted)
	action2 = BoundQualityRetry(action2, false, cfg.OnExhausted)
	if action2 != QualityActionDeliverLast {
		t.Fatalf("second withhold fail-open action=%s", action2)
	}
	got, _ := io.ReadAll(replay2)
	if !strings.Contains(string(got), content) {
		t.Fatalf("fail-open must still deliver the last body, got %q", got)
	}
}

func TestPeekQualityStreamShortDelivers(t *testing.T) {
	t.Parallel()
	body := io.NopCloser(strings.NewReader(sse(
		`data: {"choices":[{"delta":{"content":"hi"}}]}`,
		"data: [DONE]",
	)))
	replay, verdict, _, _, _, err := peekQualityStream(context.Background(), body, qualityProtocolChat, QualityRetryRuntime{MinOutputTokens: 32}, toolActionRequirement{})
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if verdict != QualityDeliver {
		t.Fatalf("short verdict=%s", verdict)
	}
}

func TestShouldHoldQualityStreamGates(t *testing.T) {
	t.Parallel()
	cfg := QualityRetryRuntime{Enabled: true, MaxAttempts: 2, MinOutputTokens: 32}
	route := modeldomain.Route{Provider: accountdomain.ProviderBuild, UpstreamModel: "grok-4.6", PublicID: "grok-4.6"}
	input := Input{Streaming: true, PublicModel: "grok-4.6"}
	if !shouldHoldQualityStream(input, nil, route, audit.OperationChat, cfg) {
		t.Fatal("expected hold on thinking build chat")
	}
	off := cfg
	off.Enabled = false
	if shouldHoldQualityStream(input, nil, route, audit.OperationChat, off) {
		t.Fatal("disabled must not hold")
	}
	forced := input
	forced.ForcedEgressNodeID = 9
	if shouldHoldQualityStream(forced, nil, route, audit.OperationChat, cfg) {
		t.Fatal("forced egress must not hold")
	}
	owned := inferencedomain.ResponseOwnership{ResponseID: "r1", AccountID: 1}
	if shouldHoldQualityStream(input, &owned, route, audit.OperationChat, cfg) {
		t.Fatal("pinned response must not hold")
	}
	if shouldHoldQualityStream(input, nil, route, audit.OperationImage, cfg) {
		t.Fatal("image must not hold")
	}
}

func TestAttemptLoopQualityHold(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "quality-hold-loop.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accountRepo := relational.NewAccountRepository(database)
	modelRepo := relational.NewModelRepository(database)
	auditRepo := relational.NewAuditRepository(database)
	responseRepo := relational.NewResponseRepository(database)
	keyRepo := relational.NewClientKeyRepository(database)

	credentials := make([]accountdomain.Credential, 0, 2)
	for index, name := range []string{"quality-a", "quality-b"} {
		credential, _, createErr := accountRepo.UpsertByIdentity(ctx, accountdomain.Credential{
			Provider: accountdomain.ProviderBuild, Name: name, SourceKey: name, EncryptedAccessToken: name,
			EncryptedRefreshToken: "refresh-" + name, ExpiresAt: time.Now().Add(time.Hour),
			Enabled: true, AuthStatus: accountdomain.AuthStatusActive, Priority: 200 - index, MaxConcurrent: 1,
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		credentials = append(credentials, credential)
	}
	if err := modelRepo.UpsertDiscovered(ctx, accountdomain.ProviderBuild, []string{"grok-4.6"}); err != nil {
		t.Fatal(err)
	}
	for _, credential := range credentials {
		if err := modelRepo.ReplaceAccountCapabilities(ctx, credential.ID, []string{"grok-4.6"}, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	clientKey, err := keyRepo.Create(ctx, clientkey.Key{
		Name: "quality-loop-key", Prefix: "qhold", SecretHash: strings.Repeat("f", 64), EncryptedSecret: "encrypted",
		Enabled: true, RPMLimit: 120, MaxConcurrent: 8,
	})
	if err != nil {
		t.Fatal(err)
	}

	noThink := sse(
		`data: {"choices":[{"delta":{"content":"`+strings.Repeat("abcd", 40)+`"}}]}`,
		`data: {"usage":{"completion_tokens":40,"completion_tokens_details":{"reasoning_tokens":0}}}`,
		"data: [DONE]",
	)
	thinking := sse(
		`data: {"choices":[{"delta":{"thinking_content":"plan the game"}}]}`,
		`data: {"choices":[{"delta":{"content":"good game after retry"}}]}`,
		`data: {"usage":{"completion_tokens":80,"completion_tokens_details":{"reasoning_tokens":40}}}`,
		"data: [DONE]",
	)
	adapter := &qualityEgressAwareBuildAdapter{
		scriptedBuildAdapter: &scriptedBuildAdapter{responses: map[uint64][]scriptedBuildResponse{
			credentials[0].ID: {{status: http.StatusOK, body: noThink}},
			credentials[1].ID: {{status: http.StatusOK, body: thinking}},
		}},
		egressIPRecordID: 901,
	}
	registry := provider.NewRegistry(adapter)
	sticky := memory.NewStickyStore()
	accountService := accountapp.NewService(accountRepo, auditRepo, memory.NewDeviceSessionStore(), sticky, registry, testCipher(t), nil)
	selector := NewSelector(accountRepo, memory.NewConcurrencyLimiter(), sticky, registry, time.Hour, time.Second, time.Minute)
	service := NewService(modelRepo, auditRepo, accountService, clientkeyapp.NewService(nil, nil, nil, 60, 4, nil), registry, selector, responseRepo, 3)
	service.UpdateQualityRetry(QualityRetryRuntime{Enabled: true, MaxAttempts: 2, MinOutputTokens: 32, AccountCooldown: 2 * time.Minute, OnExhausted: qualityRetryFailOpen, HoldTimeout: time.Second})

	result, err := service.CreateChatCompletion(ctx, Input{
		RequestID: "req-quality-hold", ClientKey: clientKey, PublicModel: "grok-4.6", Streaming: true,
		Body: []byte(`{"model":"grok-4.6","messages":[{"role":"user","content":"write a game"}],"stream":true}`),
	})
	if err != nil {
		t.Fatalf("attempt loop should deliver after withhold retry, err=%v", err)
	}
	if result.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", result.StatusCode)
	}
	body, _ := io.ReadAll(result.Body)
	result.Finalize(Usage{Reported: true, OutputTokens: 80, ReasoningTokens: 40}, "chat-ok", "")
	_ = result.Body.Close()
	if !strings.Contains(string(body), "good game after retry") || !strings.Contains(string(body), "thinking_content") {
		t.Fatalf("client must receive the second attempt body, got %s", body)
	}
	if strings.Contains(string(body), strings.Repeat("abcd", 40)) {
		t.Fatal("first no-think body must not be delivered")
	}
	attempts := adapter.Attempts()
	if len(attempts) != 2 || attempts[0] != credentials[0].ID || attempts[1] != credentials[1].ID {
		t.Fatalf("expected account exclude+retry, attempts=%#v want [%d %d]", attempts, credentials[0].ID, credentials[1].ID)
	}
	logs, total, err := auditRepo.List(ctx, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	var degraded, delivered bool
	for _, rec := range logs {
		if rec.ErrorCode == ErrorQualityDegraded && rec.AccountID != nil && *rec.AccountID == credentials[0].ID {
			degraded = true
			if rec.EgressIPRecordID == nil || *rec.EgressIPRecordID != adapter.egressIPRecordID {
				t.Fatalf("degraded audit lost egress IP attribution: %#v", rec)
			}
		}
		if rec.RequestID == "req-quality-hold" && rec.ErrorCode == "" && rec.StatusCode == http.StatusOK {
			delivered = true
		}
	}
	if !degraded {
		t.Fatalf("first withhold must write quality_degraded, audits=%d total=%d", len(logs), total)
	}
	if !adapter.retryExcluded {
		t.Fatal("quality retry did not exclude the first egress IP")
	}
	if !delivered {
		t.Fatalf("final delivered attempt missing from audits, total=%d", total)
	}
	cooled, err := accountRepo.Get(ctx, credentials[0].ID)
	if err != nil || cooled.CooldownUntil == nil || time.Until(*cooled.CooldownUntil) < time.Minute {
		t.Fatalf("missing-thinking account was not cooled: account=%#v err=%v", cooled, err)
	}
}

// qualityEgressAwareBuildAdapter simulates the transport-level attribution
// written by the real Build RoundTripper. It verifies the gateway carries a
// withheld IP exclusion into the next selected account attempt.
type qualityEgressAwareBuildAdapter struct {
	*scriptedBuildAdapter
	egressIPRecordID uint64
	retryExcluded    bool
}

func (a *qualityEgressAwareBuildAdapter) ForwardResponse(ctx context.Context, request provider.ResponseResourceRequest) (*provider.Response, error) {
	if len(a.Attempts()) > 0 && infraegress.IsBuildEgressIPRecordExcluded(ctx, a.egressIPRecordID) {
		a.retryExcluded = true
	}
	requestCtx := infraegress.WithCredential(ctx, request.Credential)
	infraegress.RecordBuildLeaseAttribution(requestCtx, &infraegress.Lease{
		NodeID: 7, NodeName: "quality-resin", Scope: "grok_build", ProxyURL: "socks5://quality",
		EgressIPLeaseID: 801, EgressIPRecordID: a.egressIPRecordID,
	})
	return a.scriptedBuildAdapter.ForwardResponse(requestCtx, request)
}

func TestNormalizeQualityRetryDefaults(t *testing.T) {
	t.Parallel()
	got := normalizeQualityRetry(QualityRetryRuntime{Enabled: true})
	if !got.Enabled || got.MaxAttempts != 2 || got.MinOutputTokens != 32 || got.AccountCooldown != 10*time.Minute || got.OnExhausted != qualityRetryFailOpen || got.HoldTimeout != 3*time.Second {
		t.Fatalf("defaults = %#v", got)
	}
}
