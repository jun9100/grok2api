package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/domain/audit"
	inferencedomain "github.com/chenyme/grok2api/backend/internal/domain/inference"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	neterrorpkg "github.com/chenyme/grok2api/backend/internal/pkg/neterror"
)

const (
	defaultZeroTokenRetryMaxAttempts = 2
	maxZeroTokenEgressSelectionSkips = 16
)

var errZeroTokenStreamIncomplete = errors.New("upstream stream ended before first output")

// ZeroTokenRetryRuntime controls the Build-only transparent retry that runs
// before the gateway has written an SSE frame to the client.
type ZeroTokenRetryRuntime struct {
	Enabled     bool
	MaxAttempts int
}

func normalizeZeroTokenRetry(cfg ZeroTokenRetryRuntime) ZeroTokenRetryRuntime {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultZeroTokenRetryMaxAttempts
	}
	if cfg.MaxAttempts > defaultZeroTokenRetryMaxAttempts {
		cfg.MaxAttempts = defaultZeroTokenRetryMaxAttempts
	}
	return cfg
}

func (s *Service) UpdateZeroTokenRetry(cfg ZeroTokenRetryRuntime) {
	normalized := normalizeZeroTokenRetry(cfg)
	s.zeroTokenRetry.Store(&normalized)
}

func (s *Service) zeroTokenRetryConfig() ZeroTokenRetryRuntime {
	if s == nil {
		return normalizeZeroTokenRetry(ZeroTokenRetryRuntime{})
	}
	if value := s.zeroTokenRetry.Load(); value != nil {
		return *value
	}
	return normalizeZeroTokenRetry(ZeroTokenRetryRuntime{})
}

func shouldHoldZeroTokenStream(input Input, ownership *inferencedomain.ResponseOwnership, route modeldomain.Route, operation audit.Operation, cfg ZeroTokenRetryRuntime) bool {
	if !cfg.Enabled || cfg.MaxAttempts < 2 || !input.Streaming || ownership != nil || input.ForcedEgressNodeID != 0 {
		return false
	}
	if route.Provider != accountdomain.ProviderBuild {
		return false
	}
	switch operation {
	case audit.OperationChat, audit.OperationResponses, audit.OperationMessages:
		return true
	default:
		return false
	}
}

func isRetryableZeroTokenStreamFailure(ctx context.Context, err error) bool {
	if err == nil || (ctx != nil && ctx.Err() != nil) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || neterrorpkg.IsUpstreamStreamIdleTimeout(err) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) && (networkError.Timeout() || networkError.Temporary()) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "connection reset") ||
		strings.Contains(message, "connection aborted") ||
		strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "unexpected eof") ||
		strings.Contains(message, "client connection lost")
}

func newZeroTokenStreamFailure(err error, accountID uint64, accountName string) *UpstreamFailure {
	failure := newTransportUpstreamFailure(err, accountID, accountName)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		failure.HTTPStatus = http.StatusBadGateway
		failure.Code = "upstream_stream_incomplete"
		failure.PublicMessage = "上游流式响应在首个输出前结束"
		failure.Fingerprint = failure.Code
		return failure
	}
	if failure.Code == "upstream_network_error" {
		failure.Code = "upstream_stream_interrupted"
		failure.PublicMessage = "上游流式响应在首个输出前中断"
		failure.Fingerprint = failure.Code
	}
	return failure
}

func (s *Service) recordZeroTokenStreamFailure(ctx context.Context, base audit.Record, credential accountdomain.Credential, statusCode int, failure *UpstreamFailure, attempts []audit.Attempt, startedAt time.Time, trace *infraegress.Trace, providerValue accountdomain.Provider) {
	if failure == nil {
		return
	}
	record := base
	record.EventID = newAuditEventID()
	accountID := credential.ID
	record.AccountID = &accountID
	record.AccountName = credential.Name
	record.StatusCode = statusCode
	record.ErrorCode = failure.AuditCode()
	record.Attempts = attempts
	record.DurationMS = time.Since(startedAt).Milliseconds()
	record.CreatedAt = time.Now().UTC()
	applyAuditEgress(&record, trace, providerValue)
	if err := s.audits.Create(ctx, record); err != nil {
		s.logger.Error("zero_token_stream_retry_audit_failed", "event_id", record.EventID, "request_id", record.RequestID, "error", err)
	}
}
