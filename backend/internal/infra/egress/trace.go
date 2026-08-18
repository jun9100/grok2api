package egress

import (
	"context"
	"fmt"
	"strings"
	"sync"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	domain "github.com/chenyme/grok2api/backend/internal/domain/egress"
)

// Selection is the egress snapshot actually selected for an upstream request. It contains only metadata safe for audit
// and excludes proxy URLs, credentials, User-Agent, and Cookies.
type Selection struct {
	NodeID             uint64
	NodeName           string
	Scope              domain.Scope
	Proxied            bool
	AccountID          uint64
	BuildBotFlagSource int
	EgressIPLeaseID    uint64
	EgressIPRecordID   uint64
}

// Trace retains the most recent actual egress selection per scope. When a request retries egress, audit records the final attempt.
// Web asset archival uses an independent scope and does not overwrite the primary Grok Web inference egress.
type Trace struct {
	mu         sync.RWMutex
	selections map[domain.Scope]Selection
}

type traceContextKey struct{}
type accountContextKey struct{}
type credentialContextKey struct{}
type buildRiskModelContextKey struct{}
type egressNodeContextKey struct{}
type qualityProbeContextKey struct{}
type excludedBuildEgressIPRecordContextKey struct{}

type excludedBuildEgressIPRecords map[uint64]struct{}

// WithAccount passes a stable Provider account identity to the egress layer. It is used only to render
// authentication usernames for sticky proxies such as Resin and is never written to upstream headers or audit.
func WithAccount(ctx context.Context, provider string, accountID uint64) context.Context {
	if ctx == nil || strings.TrimSpace(provider) == "" || accountID == 0 {
		return ctx
	}
	return WithAccountIdentity(ctx, strings.TrimSpace(provider)+"_"+fmt.Sprintf("%d", accountID))
}

// WithCredential passes the stable egress identity of a weakly linked account to Build transport;
// unlinked accounts retain the existing Provider+ID identity.
func WithCredential(ctx context.Context, credential accountdomain.Credential) context.Context {
	if ctx == nil {
		return ctx
	}
	ctx = context.WithValue(ctx, credentialContextKey{}, credential)
	identity := strings.TrimSpace(credential.EgressIdentity)
	if identity == "" {
		provider := credential.Provider
		if provider == "" {
			provider = accountdomain.ProviderBuild
		}
		return WithEgressNode(WithAccount(ctx, string(provider), credential.ID), credential.EgressNodeID)
	}
	return WithEgressNode(WithAccountIdentity(ctx, identity), credential.EgressNodeID)
}

func credentialFromContext(ctx context.Context) (accountdomain.Credential, bool) {
	if ctx == nil {
		return accountdomain.Credential{}, false
	}
	credential, ok := ctx.Value(credentialContextKey{}).(accountdomain.Credential)
	return credential, ok && credential.ID != 0
}

func WithBuildRiskModel(ctx context.Context, model string) context.Context {
	if ctx == nil || strings.TrimSpace(model) == "" {
		return ctx
	}
	return context.WithValue(ctx, buildRiskModelContextKey{}, strings.TrimSpace(model))
}

func buildRiskModelFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(buildRiskModelContextKey{}).(string)
	return strings.TrimSpace(value)
}

// WithExcludedBuildEgressIPRecord attaches a request-scoped exclusion used by
// the zero-token recovery path. It holds only an internal IP-record ID, never
// an address or proxy credential.
func WithExcludedBuildEgressIPRecord(ctx context.Context, recordID uint64) context.Context {
	if ctx == nil || recordID == 0 {
		return ctx
	}
	records := excludedBuildEgressIPRecords{}
	if existing, ok := ctx.Value(excludedBuildEgressIPRecordContextKey{}).(excludedBuildEgressIPRecords); ok {
		for value := range existing {
			records[value] = struct{}{}
		}
	}
	records[recordID] = struct{}{}
	return context.WithValue(ctx, excludedBuildEgressIPRecordContextKey{}, records)
}

// IsBuildEgressIPRecordExcluded reports whether a Build lease may use recordID
// for this request. The constraint is intentionally request-local and does
// not change durable lease or IP health state.
func IsBuildEgressIPRecordExcluded(ctx context.Context, recordID uint64) bool {
	if ctx == nil || recordID == 0 {
		return false
	}
	switch value := ctx.Value(excludedBuildEgressIPRecordContextKey{}).(type) {
	case excludedBuildEgressIPRecords:
		_, ok := value[recordID]
		return ok
	case uint64:
		// Retain compatibility with contexts created by a pre-upgrade process.
		return value != 0 && value == recordID
	default:
		return false
	}
}

// WithEgressNode attaches the explicitly assigned node ID for transports that
// only receive a request context (notably Grok Build's RoundTripper).
func WithEgressNode(ctx context.Context, nodeID uint64) context.Context {
	if ctx == nil || nodeID == 0 {
		return ctx
	}
	return context.WithValue(ctx, egressNodeContextKey{}, nodeID)
}

func egressNodeFromContext(ctx context.Context) uint64 {
	if ctx == nil {
		return 0
	}
	value, _ := ctx.Value(egressNodeContextKey{}).(uint64)
	return value
}

// EgressNodeFromContext exposes a non-sensitive binding identifier to the
// Build transport without exposing the context key itself.
func EgressNodeFromContext(ctx context.Context) uint64 { return egressNodeFromContext(ctx) }

// WithQualityProbe marks an administrator-initiated probe. It permits the
// selected fixed node to be tested while disabled or cooling without making
// that node eligible for ordinary inference traffic.
func WithQualityProbe(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, qualityProbeContextKey{}, true)
}

// QualityProbeFromContext reports whether the request is an internal
// administrator quality probe. Gateway retry policy uses this signal to keep
// ambiguous egress failures from changing credential health.
func QualityProbeFromContext(ctx context.Context) bool {
	return qualityProbeFromContext(ctx)
}

func qualityProbeFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	value, _ := ctx.Value(qualityProbeContextKey{}).(bool)
	return value
}

// WithAccountIdentity attaches the stable, non-sensitive identity used by
// account-bound proxy templates such as Resin. Providers that represent the
// same upstream login (for example Web and Console sharing one SSO token) can
// deliberately pass the same identity so their proxy and clearance lease is
// not split by the internal provider name.
func WithAccountIdentity(ctx context.Context, identity string) context.Context {
	if ctx == nil || strings.TrimSpace(identity) == "" {
		return ctx
	}
	return context.WithValue(ctx, accountContextKey{}, strings.TrimSpace(identity))
}

func accountFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(accountContextKey{}).(string)
	return strings.TrimSpace(value)
}

// AccountFromContext exposes the non-sensitive sticky account identity to
// provider transports while keeping the context key private.
func AccountFromContext(ctx context.Context) string { return accountFromContext(ctx) }

// WithTrace creates or reuses a concurrency-safe egress selection trace for one gateway request.
func WithTrace(ctx context.Context) (context.Context, *Trace) {
	if existing := TraceFromContext(ctx); existing != nil {
		return ctx, existing
	}
	trace := &Trace{selections: make(map[domain.Scope]Selection)}
	return context.WithValue(ctx, traceContextKey{}, trace), trace
}

// TraceFromContext returns the egress trace from context, or nil when none is configured.
func TraceFromContext(ctx context.Context) *Trace {
	if ctx == nil {
		return nil
	}
	trace, _ := ctx.Value(traceContextKey{}).(*Trace)
	return trace
}

// Selection returns a safe snapshot of the most recent actual egress selection for a scope.
func (t *Trace) Selection(scope domain.Scope) (Selection, bool) {
	if t == nil {
		return Selection{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	value, ok := t.selections[scope]
	return value, ok
}

func recordSelection(ctx context.Context, value Selection) {
	trace := TraceFromContext(ctx)
	if trace == nil {
		return
	}
	trace.mu.Lock()
	trace.selections[value.Scope] = value
	trace.mu.Unlock()
}

// RecordBuildLeaseAttribution enriches the trace after the Build transport has
// acquired a durable IP lease. The fields are opaque database identifiers and
// never include a proxy URL, credentials, or the actual exit address.
func RecordBuildLeaseAttribution(ctx context.Context, lease *Lease) {
	if lease == nil {
		return
	}
	credential, ok := credentialFromContext(ctx)
	if !ok || credential.Provider != accountdomain.ProviderBuild {
		return
	}
	trace := TraceFromContext(ctx)
	if trace == nil {
		return
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	selection, ok := trace.selections[domain.ScopeBuild]
	if !ok {
		selection = Selection{NodeID: lease.NodeID, NodeName: lease.NodeName, Scope: domain.ScopeBuild, Proxied: lease.ProxyURL != ""}
	}
	if selection.NodeID != 0 && lease.NodeID != 0 && selection.NodeID != lease.NodeID {
		return
	}
	selection.AccountID = credential.ID
	selection.BuildBotFlagSource = credential.BuildBotFlagSource
	selection.EgressIPLeaseID = lease.EgressIPLeaseID
	selection.EgressIPRecordID = lease.EgressIPRecordID
	trace.selections[domain.ScopeBuild] = selection
}
