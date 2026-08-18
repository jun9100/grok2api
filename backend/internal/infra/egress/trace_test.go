package egress

import (
	"context"
	"testing"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	domain "github.com/chenyme/grok2api/backend/internal/domain/egress"
)

func TestRecordBuildLeaseAttributionEnrichesCurrentBuildTrace(t *testing.T) {
	ctx, trace := WithTrace(context.Background())
	ctx = WithCredential(ctx, accountdomain.Credential{
		ID: 41, Provider: accountdomain.ProviderBuild, BuildBotFlagSource: 2,
	})
	recordSelection(ctx, Selection{NodeID: 7, NodeName: "resin", Scope: domain.ScopeBuild, Proxied: true})

	RecordBuildLeaseAttribution(ctx, &Lease{NodeID: 7, EgressIPLeaseID: 51, EgressIPRecordID: 61})

	selection, ok := trace.Selection(domain.ScopeBuild)
	if !ok || selection.AccountID != 41 || selection.BuildBotFlagSource != 2 || selection.EgressIPLeaseID != 51 || selection.EgressIPRecordID != 61 {
		t.Fatalf("Build selection = %#v, found=%v", selection, ok)
	}
}

func TestRecordBuildLeaseAttributionRejectsOtherNode(t *testing.T) {
	ctx, trace := WithTrace(context.Background())
	ctx = WithCredential(ctx, accountdomain.Credential{ID: 41, Provider: accountdomain.ProviderBuild})
	recordSelection(ctx, Selection{NodeID: 7, Scope: domain.ScopeBuild, Proxied: true})

	RecordBuildLeaseAttribution(ctx, &Lease{NodeID: 8, EgressIPLeaseID: 51, EgressIPRecordID: 61})

	selection, ok := trace.Selection(domain.ScopeBuild)
	if !ok || selection.EgressIPLeaseID != 0 || selection.EgressIPRecordID != 0 {
		t.Fatalf("mismatched lease changed selection = %#v, found=%v", selection, ok)
	}
}

func TestExcludedBuildEgressIPRecordsAccumulatePerRequest(t *testing.T) {
	ctx := WithExcludedBuildEgressIPRecord(context.Background(), 61)
	ctx = WithExcludedBuildEgressIPRecord(ctx, 62)
	if !IsBuildEgressIPRecordExcluded(ctx, 61) || !IsBuildEgressIPRecordExcluded(ctx, 62) || IsBuildEgressIPRecordExcluded(ctx, 63) {
		t.Fatalf("unexpected excluded Build IP records")
	}
}
