package gateway

import (
	"context"
	"errors"
	"testing"

	egressapp "github.com/chenyme/grok2api/backend/internal/application/egress"
	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

type qualityProbeRouteResolverStub struct {
	route       modeldomain.Route
	ensureCalls int
}

func (r *qualityProbeRouteResolverStub) Get(context.Context, uint64) (modeldomain.Route, error) {
	return r.route, nil
}

func (r *qualityProbeRouteResolverStub) GetByPublicID(context.Context, string) (modeldomain.Route, error) {
	if r.route.ID == 0 {
		return modeldomain.Route{}, repository.ErrNotFound
	}
	return r.route, nil
}

func (r *qualityProbeRouteResolverStub) GetByPublicIDCandidates(ctx context.Context, publicID string) ([]modeldomain.Route, error) {
	route, err := r.GetByPublicID(ctx, publicID)
	if err != nil {
		return nil, err
	}
	return []modeldomain.Route{route}, nil
}

func (r *qualityProbeRouteResolverStub) GetByProviderUpstream(context.Context, accountdomain.Provider, string) (modeldomain.Route, error) {
	return r.route, nil
}

func (r *qualityProbeRouteResolverStub) EnsureQualityProbeBuildRoute(_ context.Context, upstreamModel string) error {
	r.ensureCalls++
	r.route = modeldomain.Route{ID: 7, PublicID: "Build/" + upstreamModel, Provider: accountdomain.ProviderBuild, UpstreamModel: upstreamModel, Capability: modeldomain.CapabilityResponses, Enabled: true}
	return nil
}

func TestQualityProbeSelectionFailureCanBeIdentifiedByCaller(t *testing.T) {
	err := normalizeQualityProbeRequestError(errors.Join(ErrNoAvailableAccount, &SelectionUnavailableError{Reason: SelectionCooling}))
	if !errors.Is(err, egressapp.ErrQualityProbeNoAccount) {
		t.Fatalf("error = %v", err)
	}
}

func TestQualityProbeModelIsPinnedToBuildNamespace(t *testing.T) {
	got, ok := qualityProbeBuildPublicModel("grok-shared")
	if !ok || got != "Build/grok-shared" {
		t.Fatalf("Build probe model = %q, valid=%v", got, ok)
	}
	if _, ok := qualityProbeBuildPublicModel("Console/grok-shared"); ok {
		t.Fatal("quality probe must reject an explicitly non-Build model")
	}
}

func TestQualityProbeRepairsMissingBuildRouteBeforeSelection(t *testing.T) {
	models := &qualityProbeRouteResolverStub{}
	service := &Service{models: models}
	if err := service.ensureQualityProbeBuildRoute(context.Background(), "Build/grok-4.5", "grok-4.5"); err != nil {
		t.Fatal(err)
	}
	if models.ensureCalls != 1 || models.route.ID == 0 || models.route.Provider != accountdomain.ProviderBuild {
		t.Fatalf("ensure calls=%d route=%#v", models.ensureCalls, models.route)
	}
	if err := service.ensureQualityProbeBuildRoute(context.Background(), "Build/grok-4.5", "grok-4.5"); err != nil {
		t.Fatal(err)
	}
	if models.ensureCalls != 1 {
		t.Fatalf("existing route was repaired again: calls=%d", models.ensureCalls)
	}
}

func TestQualityProbeOutputTokensPerSecondMatchesAuditPanel(t *testing.T) {
	got := qualityProbeOutputTokensPerSecond(1335, 1200, 17320, 17100)
	// 17100ms wait vs 220ms tail: use full duration so buffered thinking is
	// not crushed into the flush window.
	want := float64(1335) * 1000 / 17320
	if got != want {
		t.Fatalf("output TPS = %v, want %v", got, want)
	}
}

func TestQualityProbeOutputTokensPerSecondIncludesReasoningTokens(t *testing.T) {
	// The panel reports completion/output tokens, including reasoning tokens.
	// 100ms tail after 1000ms wait is a short flush, so the window is duration.
	got := qualityProbeOutputTokensPerSecond(1050, 1000, 1100, 1000)
	want := float64(1050) * 1000 / 1100
	if got != want {
		t.Fatalf("output TPS = %v, want %v", got, want)
	}
}

func TestQualityProbeOutputTokensPerSecondKeepsNormalTail(t *testing.T) {
	got := qualityProbeOutputTokensPerSecond(200, 0, 2200, 200)
	if got != 100 {
		t.Fatalf("output TPS = %v, want 100", got)
	}
}

func TestQualityProbeOutputTokensPerSecondKeepsBurstWithoutReasoning(t *testing.T) {
	got := qualityProbeOutputTokensPerSecond(2000, 0, 10100, 10000)
	if got != 20000 {
		t.Fatalf("output TPS = %v, want 20000", got)
	}
}

func TestQualityProbeCountsThinkingContentAsFirstToken(t *testing.T) {
	if qualityProbeHasGeneratedDelta("", "", "", "") {
		t.Fatal("empty deltas must not mark first token")
	}
	if !qualityProbeHasGeneratedDelta("", "", "", "hmm") {
		t.Fatal("thinking_content must mark first token so thinking time stays in the generation window")
	}
	if !qualityProbeHasGeneratedDelta("", "", "hmm", "") {
		t.Fatal("reasoning_content must still mark first token")
	}
}
