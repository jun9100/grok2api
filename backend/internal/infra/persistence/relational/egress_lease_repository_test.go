package relational

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/egress"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

func TestAcquireBuildEgressIPLeaseEnforcesCapacityAndReusesAccountLease(t *testing.T) {
	ctx := context.Background()
	database := openTestDatabase(t)
	leases := NewEgressRepository(database)
	now := time.Now().UTC().Truncate(time.Millisecond)
	record := createEgressIPLeaseRecord(t, database, now, "198.51.100.10")

	for accountID := uint64(1); accountID <= egress.DefaultBuildIPLeaseCapacity; accountID++ {
		lease, created, err := leases.AcquireBuildEgressIPLease(ctx, egress.IPLeaseAcquireInput{
			IPRecordID: record.ID, AccountID: accountID, EgressNodeID: 7, MaxAccounts: egress.DefaultBuildIPLeaseCapacity,
			Now: now, ExpiresAt: now.Add(30 * time.Minute),
		})
		if err != nil || !created || lease.State != egress.IPLeaseStateActive || lease.AccountID != accountID || lease.LastVerifiedAt == nil || !lease.LastVerifiedAt.Equal(now) {
			t.Fatalf("account %d lease = %#v, created=%v, err=%v", accountID, lease, created, err)
		}
	}
	if _, _, err := leases.AcquireBuildEgressIPLease(ctx, egress.IPLeaseAcquireInput{
		IPRecordID: record.ID, AccountID: 4, MaxAccounts: egress.DefaultBuildIPLeaseCapacity, Now: now, ExpiresAt: now.Add(30 * time.Minute),
	}); !errors.Is(err, repository.ErrEgressIPCapacity) {
		t.Fatalf("full record error = %v", err)
	}
	lease, created, err := leases.AcquireBuildEgressIPLease(ctx, egress.IPLeaseAcquireInput{
		IPRecordID: record.ID, AccountID: 1, MaxAccounts: egress.DefaultBuildIPLeaseCapacity, Now: now, ExpiresAt: now.Add(time.Hour),
	})
	if err != nil || created || lease.ExpiresAt != now.Add(30*time.Minute) {
		t.Fatalf("existing lease = %#v, created=%v, err=%v", lease, created, err)
	}
	assertEgressIPLeaseCount(t, database, record.ID, 3)

	if err := leases.ReleaseEgressIPLease(ctx, lease.ID, "operator release", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertEgressIPLeaseCount(t, database, record.ID, 2)
	if _, created, err := leases.AcquireBuildEgressIPLease(ctx, egress.IPLeaseAcquireInput{
		IPRecordID: record.ID, AccountID: 4, MaxAccounts: egress.DefaultBuildIPLeaseCapacity, Now: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour),
	}); err != nil || !created {
		t.Fatalf("lease after release created=%v, err=%v", created, err)
	}
	assertEgressIPLeaseCount(t, database, record.ID, 3)
}

func TestAcquireBuildEgressIPLeaseExpiresOldLeaseBeforeClaimingSlot(t *testing.T) {
	ctx := context.Background()
	database := openTestDatabase(t)
	leases := NewEgressRepository(database)
	now := time.Now().UTC().Truncate(time.Millisecond)
	record := createEgressIPLeaseRecord(t, database, now, "198.51.100.11")
	first, created, err := leases.AcquireBuildEgressIPLease(ctx, egress.IPLeaseAcquireInput{
		IPRecordID: record.ID, AccountID: 10, MaxAccounts: 1, Now: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil || !created {
		t.Fatalf("first lease = %#v, created=%v, err=%v", first, created, err)
	}
	second, created, err := leases.AcquireBuildEgressIPLease(ctx, egress.IPLeaseAcquireInput{
		IPRecordID: record.ID, AccountID: 11, MaxAccounts: 1, Now: now.Add(2 * time.Minute), ExpiresAt: now.Add(3 * time.Minute),
	})
	if err != nil || !created || second.AccountID != 11 {
		t.Fatalf("lease after expiration = %#v, created=%v, err=%v", second, created, err)
	}
	assertEgressIPLeaseCount(t, database, record.ID, 1)
	var expired egressIPLeaseModel
	if err := database.db.First(&expired, first.ID).Error; err != nil {
		t.Fatal(err)
	}
	if expired.State != string(egress.IPLeaseStateExpired) || expired.ReleaseReason != "lease expired" {
		t.Fatalf("expired lease = %#v", expired)
	}
}

func TestAcquireBuildEgressIPLeaseRejectsNonBuildIPv4Records(t *testing.T) {
	ctx := context.Background()
	database := openTestDatabase(t)
	leases := NewEgressRepository(database)
	now := time.Now().UTC().Truncate(time.Millisecond)
	record := createEgressIPLeaseRecord(t, database, now, "2001:db8::10")
	record.Family = "ipv6"
	if err := database.db.Save(&record).Error; err != nil {
		t.Fatal(err)
	}
	if _, _, err := leases.AcquireBuildEgressIPLease(ctx, egress.IPLeaseAcquireInput{
		IPRecordID: record.ID, AccountID: 1, MaxAccounts: 1, Now: now, ExpiresAt: now.Add(time.Minute),
	}); !errors.Is(err, repository.ErrInvalidEgressIPLease) {
		t.Fatalf("IPv6 record error = %v", err)
	}
}

func TestAcquireBuildEgressIPLeaseConcurrentCapacity(t *testing.T) {
	ctx := context.Background()
	database := openTestDatabase(t)
	leases := NewEgressRepository(database)
	now := time.Now().UTC().Truncate(time.Millisecond)
	record := createEgressIPLeaseRecord(t, database, now, "198.51.100.12")

	start := make(chan struct{})
	errorsByAccount := make(chan error, 10)
	var workers sync.WaitGroup
	for accountID := uint64(1); accountID <= 10; accountID++ {
		workers.Add(1)
		go func(accountID uint64) {
			defer workers.Done()
			<-start
			_, _, err := leases.AcquireBuildEgressIPLease(ctx, egress.IPLeaseAcquireInput{
				IPRecordID: record.ID, AccountID: accountID, MaxAccounts: egress.DefaultBuildIPLeaseCapacity,
				Now: now, ExpiresAt: now.Add(30 * time.Minute),
			})
			errorsByAccount <- err
		}(accountID)
	}
	close(start)
	workers.Wait()
	close(errorsByAccount)

	created := 0
	for err := range errorsByAccount {
		switch {
		case err == nil:
			created++
		case errors.Is(err, repository.ErrEgressIPCapacity):
		default:
			t.Fatalf("concurrent acquisition error = %v", err)
		}
	}
	if created != egress.DefaultBuildIPLeaseCapacity {
		t.Fatalf("concurrent created = %d, want %d", created, egress.DefaultBuildIPLeaseCapacity)
	}
	assertEgressIPLeaseCount(t, database, record.ID, egress.DefaultBuildIPLeaseCapacity)
	var active int64
	if err := database.db.Model(&egressIPLeaseModel{}).Where("ip_record_id = ? AND state = ?", record.ID, string(egress.IPLeaseStateActive)).Count(&active).Error; err != nil {
		t.Fatal(err)
	}
	if active != int64(egress.DefaultBuildIPLeaseCapacity) {
		t.Fatalf("concurrent active leases = %d, want %d", active, egress.DefaultBuildIPLeaseCapacity)
	}
}

func TestObserveBuildEgressRequestAggregatesUsageAndSnapshotsAccountRisk(t *testing.T) {
	ctx := context.Background()
	database := openTestDatabase(t)
	leases := NewEgressRepository(database)
	now := time.Date(2026, time.August, 17, 10, 7, 0, 0, time.UTC)
	record := createEgressIPLeaseRecord(t, database, now, "198.51.100.13")
	lease, created, err := leases.AcquireBuildEgressIPLease(ctx, egress.IPLeaseAcquireInput{
		IPRecordID: record.ID, AccountID: 21, MaxAccounts: 3, Now: now, ExpiresAt: now.Add(time.Hour),
	})
	if err != nil || !created {
		t.Fatalf("lease=%#v created=%v err=%v", lease, created, err)
	}
	for _, status := range []int{200, 429} {
		if err := leases.ObserveBuildEgressRequest(ctx, egress.BuildRequestObservation{
			LeaseID: lease.ID, AccountID: 21, BotFlagSource: 2, StatusCode: status, ObservedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	var risk buildAccountRiskObservationModel
	if err := database.db.Where("egress_ip_lease_id = ?", lease.ID).Take(&risk).Error; err != nil {
		t.Fatal(err)
	}
	if risk.IPRecordID != record.ID || risk.AccountID != 21 || risk.BotFlagSource != 2 || !risk.ObservedAt.Equal(now) {
		t.Fatalf("risk=%#v", risk)
	}
	var usage buildEgressUsageWindowModel
	if err := database.db.Where("ip_record_id = ? AND account_id = ?", record.ID, 21).Take(&usage).Error; err != nil {
		t.Fatal(err)
	}
	if usage.RequestCount != 2 || usage.LastStatusCode != 429 || !usage.WindowStartedAt.Equal(now.Truncate(5*time.Minute)) {
		t.Fatalf("usage=%#v", usage)
	}
}

func TestListBuildEgressRiskSummariesKeepsEmptyObservationWindows(t *testing.T) {
	ctx := context.Background()
	database := openTestDatabase(t)
	leases := NewEgressRepository(database)
	now := time.Date(2026, time.August, 17, 10, 7, 0, 0, time.UTC)
	observed := createEgressIPLeaseRecord(t, database, now, "198.51.100.14")
	empty := createEgressIPLeaseRecord(t, database, now.Add(time.Second), "198.51.100.15")

	lease, created, err := leases.AcquireBuildEgressIPLease(ctx, egress.IPLeaseAcquireInput{
		IPRecordID: observed.ID, AccountID: 31, MaxAccounts: 3, Now: now, ExpiresAt: now.Add(time.Hour),
	})
	if err != nil || !created {
		t.Fatalf("lease=%#v created=%v err=%v", lease, created, err)
	}
	if err := leases.ObserveBuildEgressRequest(ctx, egress.BuildRequestObservation{
		LeaseID: lease.ID, AccountID: 31, BotFlagSource: 0, StatusCode: 200, ObservedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := leases.ObserveBuildEgressOutcome(ctx, egress.BuildResponseOutcome{
		LeaseID: lease.ID, AccountID: 31, Model: "grok-4.5", StatusCode: 200, ObservedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	summaries, err := leases.ListBuildEgressRiskSummaries(ctx, now.Add(-time.Minute), 10)
	if err != nil {
		t.Fatalf("list summaries: %v", err)
	}
	byIP := make(map[string]egress.BuildEgressRiskSummary, len(summaries))
	for _, summary := range summaries {
		byIP[summary.ExitIP] = summary
	}
	if got := byIP[observed.ExitIP]; got.WindowRequestCount != 1 || got.CompletedResponseCount != 1 || got.LastObservedAt.IsZero() {
		t.Fatalf("observed summary=%#v", got)
	}
	if got := byIP[empty.ExitIP]; got.WindowRequestCount != 0 || got.CompletedResponseCount != 0 || !got.LastObservedAt.IsZero() {
		t.Fatalf("empty summary=%#v", got)
	}
}

func createEgressIPLeaseRecord(t *testing.T, database *Database, now time.Time, exitIP string) egressIPRecordModel {
	t.Helper()
	record := egressIPRecordModel{
		Scope: string(egress.ScopeBuild), Family: "ipv4", ExitIP: exitIP,
		FirstSeenAt: now, LastSeenAt: now, LastProbeAt: now, LastNodeID: 7,
		ProbeStatus: string(egress.ProbeStatusHealthy), ProbeProvider: string(egress.ProbeProviderCloudflare),
	}
	if err := database.db.Create(&record).Error; err != nil {
		t.Fatal(err)
	}
	return record
}

func assertEgressIPLeaseCount(t *testing.T, database *Database, recordID uint64, want uint64) {
	t.Helper()
	var record egressIPRecordModel
	if err := database.db.First(&record, recordID).Error; err != nil {
		t.Fatal(err)
	}
	if record.ActiveLeaseCount != want {
		t.Fatalf("record %d active lease count = %d, want %d", recordID, record.ActiveLeaseCount, want)
	}
}
