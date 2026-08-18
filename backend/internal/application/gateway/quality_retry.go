package gateway

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/domain/audit"
	inferencedomain "github.com/chenyme/grok2api/backend/internal/domain/inference"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
)

const (
	ErrorQualityDegraded          = "quality_degraded"
	ErrorToolActionUnverified     = "tool_action_unverified"
	ErrorAgentContractUnfulfilled = "agent_contract_unfulfilled"
	ErrorAgentOutcomeUnverified   = "agent_outcome_unverified"
	ErrorAgentStall               = "agent_stall"
	qualityRetryFailOpen          = "fail_open"
	qualityRetryFailClosed        = "fail_closed"
	defaultQualityMaxAttempts     = 2
	defaultQualityHoldTimeout     = 3 * time.Second
	defaultQualityMinOutput       = int64(32)
	defaultQualityAccountCooldown = 10 * time.Minute
)

var (
	errQualityDegraded      = errors.New("上游响应缺少推理")
	errToolActionUnverified = errors.New("上游工具动作未验证")
	errAgentContract        = errors.New("Agent 任务契约未满足")
	errAgentOutcome         = errors.New("Agent 任务结果未验证")
	errAgentStall           = errors.New("Agent 工具轨迹停滞")
)

// QualityRetryRuntime is the isolated request-path withhold/retry policy.
// Zero Enabled leaves production behavior unchanged.
type QualityRetryRuntime struct {
	Enabled         bool
	MaxAttempts     int
	HoldTimeout     time.Duration
	MinOutputTokens int64
	// AccountCooldown keeps a Build account out of the selector after a
	// confirmed missing-thinking response. It is independent from the local
	// request retry so a marked account is not immediately reused elsewhere.
	AccountCooldown time.Duration
	OnExhausted     string
	// ToolActionGuard enables strict Anthropic Agent handling for file-like
	// artifact tasks. The stream is held until terminal so a final claim can be
	// checked against prior Write/Edit/Bash tool results.
	ToolActionGuard bool
	// AgentOutcomeGuard enables the protocol-neutral tool ledger. It accepts
	// Anthropic Messages, OpenAI Responses, and Chat Completions histories.
	AgentOutcomeGuard bool
	// AgentStallTurns is the minimum number of observation-only turns before a
	// structured contract can be classified as stalled.
	AgentStallTurns int
}

// QualityStreamSignals is the hold classifier input. Tests drive this
// directly and via ObserveQualityChunk on SSE fixtures.
type QualityStreamSignals struct {
	HasThinking     bool
	HasToolUse      bool
	VisibleTokens   int64
	ReasoningTokens int64
	OutputTokens    int64
	Terminal        bool
	HoldExpired     bool
}

// QualityVerdict is the hold decision for one upstream stream.
type QualityVerdict string

const (
	QualityWait     QualityVerdict = "wait"
	QualityDeliver  QualityVerdict = "deliver"
	QualityWithhold QualityVerdict = "withhold"
)

// QualityRetryAction is what the attempt loop does with a withhold verdict.
type QualityRetryAction string

const (
	QualityActionDeliver     QualityRetryAction = "deliver"
	QualityActionDeliverLast QualityRetryAction = "deliver_last"
	QualityActionRetry       QualityRetryAction = "retry"
	QualityActionReject      QualityRetryAction = "reject"
)

func normalizeQualityRetry(cfg QualityRetryRuntime) QualityRetryRuntime {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultQualityMaxAttempts
	}
	if cfg.HoldTimeout <= 0 {
		cfg.HoldTimeout = defaultQualityHoldTimeout
	}
	if cfg.MinOutputTokens <= 0 {
		cfg.MinOutputTokens = defaultQualityMinOutput
	}
	if cfg.AccountCooldown <= 0 {
		cfg.AccountCooldown = defaultQualityAccountCooldown
	}
	if cfg.AgentStallTurns <= 0 {
		cfg.AgentStallTurns = 6
	}
	switch strings.TrimSpace(strings.ToLower(cfg.OnExhausted)) {
	case qualityRetryFailClosed:
		cfg.OnExhausted = qualityRetryFailClosed
	default:
		cfg.OnExhausted = qualityRetryFailOpen
	}
	return cfg
}

func (s *Service) UpdateQualityRetry(cfg QualityRetryRuntime) {
	normalized := normalizeQualityRetry(cfg)
	s.qualityRetry.Store(&normalized)
}

func (s *Service) qualityRetryConfig() QualityRetryRuntime {
	if s == nil {
		return normalizeQualityRetry(QualityRetryRuntime{})
	}
	if value := s.qualityRetry.Load(); value != nil {
		return *value
	}
	return normalizeQualityRetry(QualityRetryRuntime{})
}

// ClassifyQualityHold decides whether a held stream may be forwarded.
// Thinking (or reasoning tokens) always delivers. A tool call also delivers:
// it is executable client work rather than a final answer, and the agent
// outcome guard separately validates its structured follow-up. A finished or
// expired text sample with enough visible output and no reasoning is 降智 and
// withheld. Short replies below minOutput are delivered so "ok"/"yes" is not
// retried.
func ClassifyQualityHold(sig QualityStreamSignals, minOutput int64) QualityVerdict {
	if minOutput <= 0 {
		minOutput = defaultQualityMinOutput
	}
	if sig.HasThinking || sig.ReasoningTokens > 0 || sig.HasToolUse {
		return QualityDeliver
	}
	output := sig.OutputTokens
	if output < sig.VisibleTokens {
		output = sig.VisibleTokens
	}
	enough := output >= minOutput
	if sig.Terminal {
		if enough {
			return QualityWithhold
		}
		return QualityDeliver
	}
	if enough {
		return QualityWithhold
	}
	if sig.HoldExpired {
		return QualityDeliver
	}
	return QualityWait
}

// DecideQualityRetry caps withhold recovery at maxAttempts (default 2:
// original + one extra account). The last withhold
// (attemptIndex == maxAttempts-1) is fail-open unless OnExhausted is fail_closed.
func DecideQualityRetry(verdict QualityVerdict, attemptIndex, maxAttempts int, onExhausted string) QualityRetryAction {
	if verdict != QualityWithhold {
		return QualityActionDeliver
	}
	if maxAttempts <= 0 {
		maxAttempts = defaultQualityMaxAttempts
	}
	if attemptIndex < 0 {
		attemptIndex = 0
	}
	if attemptIndex < maxAttempts-1 {
		return QualityActionRetry
	}
	// attemptIndex == maxAttempts-1 (or past it): do not retry again.
	if strings.EqualFold(strings.TrimSpace(onExhausted), qualityRetryFailClosed) {
		return QualityActionReject
	}
	return QualityActionDeliverLast
}

// BoundQualityRetry turns a Retry into DeliverLast/Reject when the routing
// loop has no remaining account slot, so the already-held body is not dropped
// on continue-into-exhausted-loop.
func BoundQualityRetry(action QualityRetryAction, hasNextRoutingAttempt bool, onExhausted string) QualityRetryAction {
	if action != QualityActionRetry || hasNextRoutingAttempt {
		return action
	}
	if strings.EqualFold(strings.TrimSpace(onExhausted), qualityRetryFailClosed) {
		return QualityActionReject
	}
	return QualityActionDeliverLast
}

// QualityCommit is the single attempt-loop decision for a held stream.
type QualityCommit struct {
	Action   QualityRetryAction
	Audit    bool
	KeepBody bool
}

// CommitQualityHold is the shipped withhold/retry/commit unit. The attempt
// loop must not re-derive this from Decide+Bound+switch.
func CommitQualityHold(verdict QualityVerdict, qualityAttempt, maxAttempts int, hasNextRouting bool, onExhausted string) QualityCommit {
	action := BoundQualityRetry(
		DecideQualityRetry(verdict, qualityAttempt, maxAttempts, onExhausted),
		hasNextRouting,
		onExhausted,
	)
	switch action {
	case QualityActionRetry, QualityActionReject:
		return QualityCommit{Action: action, Audit: true, KeepBody: false}
	case QualityActionDeliverLast:
		// A fail-open policy still needs a durable quality signal. Otherwise a
		// known degraded last response would be invisible to account/IP recovery.
		return QualityCommit{Action: action, Audit: true, KeepBody: true}
	default:
		return QualityCommit{Action: QualityActionDeliver, Audit: false, KeepBody: true}
	}
}

func shouldHoldQualityStream(input Input, ownership *inferencedomain.ResponseOwnership, route modeldomain.Route, operation audit.Operation, cfg QualityRetryRuntime) bool {
	if !cfg.Enabled || !input.Streaming || input.ForcedEgressNodeID != 0 || ownership != nil {
		return false
	}
	switch operation {
	case audit.OperationChat, audit.OperationResponses, audit.OperationMessages, "":
	default:
		return false
	}
	if route.Provider != accountdomain.ProviderBuild && route.Provider != accountdomain.ProviderConsole {
		return false
	}
	if modeldomain.SupportsReasoningForProvider(route.Provider, input.PublicModel) {
		return true
	}
	return modeldomain.SupportsReasoningForProvider(route.Provider, route.UpstreamModel)
}

func shouldHoldToolActionStream(input Input, ownership *inferencedomain.ResponseOwnership, route modeldomain.Route, operation audit.Operation, cfg QualityRetryRuntime, requirement toolActionRequirement) bool {
	if !cfg.Enabled || (!cfg.ToolActionGuard && !cfg.AgentOutcomeGuard) || !requirement.Enabled || !input.Streaming || input.ForcedEgressNodeID != 0 || ownership != nil || route.Provider != accountdomain.ProviderBuild {
		return false
	}
	switch operation {
	case audit.OperationMessages, audit.OperationResponses, audit.OperationChat:
		return true
	default:
		return false
	}
}

func qualityFailureError(code string) error {
	if code == ErrorAgentContractUnfulfilled {
		return errAgentContract
	}
	if code == ErrorAgentOutcomeUnverified {
		return errAgentOutcome
	}
	if code == ErrorAgentStall {
		return errAgentStall
	}
	if code == ErrorToolActionUnverified {
		return errToolActionUnverified
	}
	return errQualityDegraded
}

// agentOutcomeValidationFailure is a client-visible proof failure, not an
// upstream availability failure. Retrying the same completed tool history on
// more accounts cannot make its artifact receipt become valid.
func agentOutcomeValidationFailure(code string) bool {
	switch code {
	case ErrorAgentContractUnfulfilled, ErrorAgentOutcomeUnverified:
		return true
	default:
		return false
	}
}

func qualityFailureHTTPStatus(code string) int {
	if agentOutcomeValidationFailure(code) {
		return http.StatusUnprocessableEntity
	}
	return http.StatusServiceUnavailable
}

func qualityFailureMessage(code string) string {
	if code == ErrorAgentContractUnfulfilled {
		return "Agent 任务契约未满足"
	}
	if code == ErrorAgentOutcomeUnverified {
		return "Agent 任务结果未验证"
	}
	if code == ErrorAgentStall {
		return "Agent 工具轨迹停滞"
	}
	if code == ErrorToolActionUnverified {
		return "上游工具动作未验证"
	}
	return "上游响应缺少推理"
}

func (s *Service) recordQualityDegraded(ctx context.Context, base audit.Record, credential accountdomain.Credential, usage Usage, startedAt time.Time, trace *infraegress.Trace, provider accountdomain.Provider, errorCode string) {
	if strings.TrimSpace(errorCode) == "" {
		errorCode = ErrorQualityDegraded
	}
	record := base
	record.EventID = newAuditEventID()
	accountID := credential.ID
	record.AccountID = &accountID
	record.AccountName = credential.Name
	record.StatusCode = http.StatusOK
	record.ErrorCode = errorCode
	record.OutputTokens = usage.OutputTokens
	record.ReasoningTokens = usage.ReasoningTokens
	record.TotalTokens = usage.TotalTokens
	record.InputTokens = usage.InputTokens
	if usage.Reported {
		record.UsageSource = audit.UsageSourceUpstream
	}
	record.DurationMS = time.Since(startedAt).Milliseconds()
	record.CreatedAt = time.Now().UTC()
	applyAuditEgress(&record, trace, provider)
	if err := s.audits.Create(ctx, record); err != nil {
		s.logger.Error("quality_degraded_audit_failed", "event_id", record.EventID, "request_id", record.RequestID, "error", err)
	}
}
