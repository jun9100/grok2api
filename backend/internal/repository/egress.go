package repository

import (
	"context"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/egress"
)

type EgressRepository interface {
	ListEgressNodes(ctx context.Context, scope egress.Scope, sort SortQuery) ([]egress.Node, error)
	GetEgressNode(ctx context.Context, id uint64) (egress.Node, error)
	CreateEgressNode(ctx context.Context, value egress.Node) (egress.Node, error)
	UpdateEgressNode(ctx context.Context, value egress.Node) (egress.Node, error)
	DeleteEgressNode(ctx context.Context, id uint64) error
}

// EgressIPLeaseRepository is separate from the routing contract while
// lease-backed Build routing remains behind a feature switch.
type EgressIPLeaseRepository interface {
	GetActiveBuildEgressIPLease(context.Context, uint64, time.Time) (egress.IPLease, error)
	ObserveBuildEgressIPv4(context.Context, uint64, egress.ProbeResult) (uint64, error)
	AcquireBuildEgressIPLease(context.Context, egress.IPLeaseAcquireInput) (egress.IPLease, bool, error)
	ReleaseEgressIPLease(context.Context, uint64, string, time.Time) error
}

// EgressNodePageRepository is the bounded management-list contract. Runtime
// routing repositories only need EgressRepository's full-list operations.
type EgressNodePageRepository interface {
	ListEgressNodePage(ctx context.Context, input EgressNodeListQuery) ([]egress.Node, int64, error)
}

type EgressNodeCleanupPreview struct {
	Nodes               int64
	BoundAccounts       int64
	SubscriptionManaged int64
}

// EgressNodeUnhealthyCleaner provides an atomic cleanup path for nodes whose
// latest IPv4 and IPv6 probes both failed.
type EgressNodeUnhealthyCleaner interface {
	PreviewUnhealthyEgressNodes(context.Context) (EgressNodeCleanupPreview, error)
	DeleteUnhealthyEgressNodes(context.Context) ([]uint64, error)
}

type EgressNodeListFilter struct {
	Scope       egress.Scope
	Enabled     *bool
	ProbeStatus egress.ProbeStatus
	Assignment  string
}

type EgressNodeListQuery struct {
	Page   PageQuery
	Filter EgressNodeListFilter
}

type EgressSourceListFilter struct {
	Scope egress.Scope
}

type EgressSourceListQuery struct {
	Page   PageQuery
	Filter EgressSourceListFilter
}
