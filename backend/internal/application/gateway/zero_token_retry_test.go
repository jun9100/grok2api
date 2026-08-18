package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	accountapp "github.com/chenyme/grok2api/backend/internal/application/account"
	clientkeyapp "github.com/chenyme/grok2api/backend/internal/application/clientkey"
	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/domain/audit"
	"github.com/chenyme/grok2api/backend/internal/domain/clientkey"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/runtime/memory"
)

type zeroTokenChunk struct {
	value string
	err   error
}

type zeroTokenReadCloser struct {
	chunks []zeroTokenChunk
	index  int
}

func (b *zeroTokenReadCloser) Read(target []byte) (int, error) {
	if b.index >= len(b.chunks) {
		return 0, io.EOF
	}
	chunk := b.chunks[b.index]
	b.index++
	return copy(target, chunk.value), chunk.err
}

func (*zeroTokenReadCloser) Close() error { return nil }

func TestPeekZeroTokenStream(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		body      io.ReadCloser
		wantError bool
		wantBody  string
	}{
		{
			name:      "interruption before output retries",
			body:      &zeroTokenReadCloser{chunks: []zeroTokenChunk{{err: errors.New("connection reset by peer")}}},
			wantError: true,
		},
		{
			name: "content prevents replay",
			body: &zeroTokenReadCloser{chunks: []zeroTokenChunk{{
				value: `data: {"choices":[{"delta":{"content":"answer"}}]}` + "\n\n",
				err:   io.ErrUnexpectedEOF,
			}}},
			wantBody: "answer",
		},
		{
			name:     "completed empty response is delivered",
			body:     io.NopCloser(strings.NewReader("data: [DONE]\n\n")),
			wantBody: "[DONE]",
		},
		{
			name:     "terminal upstream error is delivered",
			body:     io.NopCloser(strings.NewReader(`data: {"type":"response.failed","error":{"code":"rate_limit"}}` + "\n\n")),
			wantBody: "response.failed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			replay, err := peekZeroTokenStream(context.Background(), test.body, qualityProtocolChat)
			if (err != nil) != test.wantError {
				t.Fatalf("peek error = %v, want error=%t", err, test.wantError)
			}
			if replay == nil {
				t.Fatal("replay body is nil")
			}
			defer replay.Close()
			body, readErr := io.ReadAll(replay)
			if readErr != nil {
				t.Fatalf("read replay: %v", readErr)
			}
			if test.wantBody != "" && !strings.Contains(string(body), test.wantBody) {
				t.Fatalf("replay body = %q, want %q", body, test.wantBody)
			}
		})
	}
}

func TestShouldHoldZeroTokenStreamGates(t *testing.T) {
	t.Parallel()
	cfg := ZeroTokenRetryRuntime{Enabled: true, MaxAttempts: 2}
	input := Input{Streaming: true}
	route := modeldomain.Route{Provider: accountdomain.ProviderBuild}
	if !shouldHoldZeroTokenStream(input, nil, route, audit.OperationChat, cfg) {
		t.Fatal("Build chat stream should hold")
	}
	route.Provider = accountdomain.ProviderWeb
	if shouldHoldZeroTokenStream(input, nil, route, audit.OperationChat, cfg) {
		t.Fatal("Web stream must not hold")
	}
	route.Provider = accountdomain.ProviderBuild
	if shouldHoldZeroTokenStream(Input{Streaming: true, ForcedEgressNodeID: 1}, nil, route, audit.OperationChat, cfg) {
		t.Fatal("forced quality probe must not hold")
	}
	if shouldHoldZeroTokenStream(input, nil, route, audit.OperationImage, cfg) {
		t.Fatal("image operation must not hold")
	}
}

type zeroTokenRetryAttempt struct {
	accountID           uint64
	excludedFirstEgress bool
}

type zeroTokenRetryAdapter struct {
	mu              sync.Mutex
	firstAccountID  uint64
	secondAccountID uint64
	firstEgressID   uint64
	secondEgressID  uint64
	secondFails     bool
	attempts        []zeroTokenRetryAttempt
}

func (a *zeroTokenRetryAdapter) Provider() accountdomain.Provider { return accountdomain.ProviderBuild }

func (a *zeroTokenRetryAdapter) Definition() provider.Definition {
	definition := testConversationDefinition(accountdomain.ProviderBuild)
	definition.Conversation.StoredResponses = true
	return definition
}

func (a *zeroTokenRetryAdapter) ForwardResponse(ctx context.Context, request provider.ResponseResourceRequest) (*provider.Response, error) {
	a.mu.Lock()
	accountID := request.Credential.ID
	a.attempts = append(a.attempts, zeroTokenRetryAttempt{
		accountID:           accountID,
		excludedFirstEgress: infraegress.IsBuildEgressIPRecordExcluded(ctx, a.firstEgressID),
	})
	firstAccountID, firstEgressID := a.firstAccountID, a.firstEgressID
	secondEgressID, secondFails := a.secondEgressID, a.secondFails
	a.mu.Unlock()

	requestCtx := infraegress.WithCredential(ctx, request.Credential)
	egressID := secondEgressID
	if accountID == firstAccountID {
		egressID = firstEgressID
	}
	infraegress.RecordBuildLeaseAttribution(requestCtx, &infraegress.Lease{
		NodeID: 1, NodeName: "test-resin", Scope: "grok_build", ProxyURL: "socks5://test", EgressIPRecordID: egressID,
	})
	if accountID == firstAccountID || secondFails {
		return &provider.Response{
			StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
			Body: &zeroTokenReadCloser{chunks: []zeroTokenChunk{{err: errors.New("connection reset by peer")}}},
		}, nil
	}
	return &provider.Response{
		StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(
			`data: {"choices":[{"delta":{"thinking_content":"plan"}}]}` + "\n\n" +
				`data: {"choices":[{"delta":{"content":"recovered answer"}}]}` + "\n\n" +
				"data: [DONE]\n\n",
		)),
	}, nil
}

func (a *zeroTokenRetryAdapter) RefreshCredential(context.Context, accountdomain.Credential) (provider.RefreshedCredential, error) {
	return provider.RefreshedCredential{ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func (a *zeroTokenRetryAdapter) Attempts() []zeroTokenRetryAttempt {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]zeroTokenRetryAttempt(nil), a.attempts...)
}

func newZeroTokenRetryService(t *testing.T, secondFails bool) (*Service, *relational.AuditRepository, clientkey.Key, []accountdomain.Credential, *zeroTokenRetryAdapter) {
	t.Helper()
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "zero-token-retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	accountRepo := relational.NewAccountRepository(database)
	modelRepo := relational.NewModelRepository(database)
	auditRepo := relational.NewAuditRepository(database)
	responseRepo := relational.NewResponseRepository(database)
	keyRepo := relational.NewClientKeyRepository(database)

	credentials := make([]accountdomain.Credential, 0, 2)
	for index, name := range []string{"zero-token-a", "zero-token-b"} {
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
	key, err := keyRepo.Create(ctx, clientkey.Key{
		Name: "zero-token-key", Prefix: "ztr", SecretHash: strings.Repeat("a", 64), EncryptedSecret: "encrypted",
		Enabled: true, RPMLimit: 120, MaxConcurrent: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &zeroTokenRetryAdapter{
		firstAccountID: credentials[0].ID, secondAccountID: credentials[1].ID,
		firstEgressID: 101, secondEgressID: 202, secondFails: secondFails,
	}
	registry := provider.NewRegistry(adapter)
	sticky := memory.NewStickyStore()
	accountService := accountapp.NewService(accountRepo, auditRepo, memory.NewDeviceSessionStore(), sticky, registry, testCipher(t), nil)
	selector := NewSelector(accountRepo, memory.NewConcurrencyLimiter(), sticky, registry, time.Hour, time.Second, time.Minute)
	service := NewService(modelRepo, auditRepo, accountService, clientkeyapp.NewService(nil, nil, nil, 60, 4, nil), registry, selector, responseRepo, 3)
	service.UpdateZeroTokenRetry(ZeroTokenRetryRuntime{Enabled: true, MaxAttempts: 2})
	return service, auditRepo, key, credentials, adapter
}

func TestAttemptLoopZeroTokenRetrySwitchesAccountAndEgress(t *testing.T) {
	service, auditRepo, key, credentials, adapter := newZeroTokenRetryService(t, false)
	result, err := service.CreateChatCompletion(context.Background(), Input{
		RequestID: "req-zero-token-retry", ClientKey: key, PublicModel: "grok-4.6", Streaming: true,
		Body: []byte(`{"model":"grok-4.6","messages":[{"role":"user","content":"write a plan"}],"stream":true}`),
	})
	if err != nil {
		t.Fatalf("CreateChatCompletion: %v", err)
	}
	body, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	result.Finalize(Usage{Reported: true, OutputTokens: 2, ReasoningTokens: 1}, "chat-zero-token", "")
	_ = result.Body.Close()
	if !strings.Contains(string(body), "recovered answer") || strings.Contains(string(body), "connection reset") {
		t.Fatalf("client body = %q", body)
	}
	attempts := adapter.Attempts()
	if len(attempts) != 2 || attempts[0].accountID != credentials[0].ID || attempts[1].accountID != credentials[1].ID {
		t.Fatalf("attempts = %#v", attempts)
	}
	if attempts[0].excludedFirstEgress || !attempts[1].excludedFirstEgress {
		t.Fatalf("egress exclusion propagation = %#v", attempts)
	}

	logs, _, err := auditRepo.List(context.Background(), 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	var firstFailure, recovered bool
	for _, record := range logs {
		if record.RequestID != "req-zero-token-retry" {
			continue
		}
		if record.ErrorCode == "upstream_stream_interrupted" && record.AccountID != nil && *record.AccountID == credentials[0].ID {
			firstFailure = record.EgressIPRecordID != nil && *record.EgressIPRecordID == 101 && record.OutputTokens == 0 && record.FirstTokenMS == nil
		}
		if record.ErrorCode == "" && record.AccountID != nil && *record.AccountID == credentials[1].ID {
			recovered = record.EgressIPRecordID != nil && *record.EgressIPRecordID == 202
		}
	}
	if !firstFailure || !recovered {
		t.Fatalf("audits first_failure=%t recovered=%t records=%#v", firstFailure, recovered, logs)
	}
}

func TestAttemptLoopZeroTokenRetryStopsAfterSecondFailure(t *testing.T) {
	service, _, key, credentials, adapter := newZeroTokenRetryService(t, true)
	_, err := service.CreateChatCompletion(context.Background(), Input{
		RequestID: "req-zero-token-exhausted", ClientKey: key, PublicModel: "grok-4.6", Streaming: true,
		Body: []byte(`{"model":"grok-4.6","messages":[{"role":"user","content":"write a plan"}],"stream":true}`),
	})
	var failure *UpstreamFailure
	if !errors.As(err, &failure) || failure.Code != "upstream_stream_interrupted" {
		t.Fatalf("failure = %#v, err=%v", failure, err)
	}
	attempts := adapter.Attempts()
	if len(attempts) != 2 || attempts[0].accountID != credentials[0].ID || attempts[1].accountID != credentials[1].ID {
		t.Fatalf("attempts = %#v", attempts)
	}
}
