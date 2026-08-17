package relational

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/egress"
	"github.com/chenyme/grok2api/backend/internal/repository"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (r *EgressRepository) GetActiveBuildEgressIPLease(ctx context.Context, accountID uint64, now time.Time) (egress.IPLease, error) {
	if accountID == 0 {
		return egress.IPLease{}, repository.ErrInvalidEgressIPLease
	}
	if now = now.UTC(); now.IsZero() {
		now = time.Now().UTC()
	}
	var row egressIPLeaseModel
	if err := r.db.db.WithContext(ctx).
		Where("account_id = ? AND scope = ? AND state = ? AND expires_at > ?", accountID, string(egress.ScopeBuild), string(egress.IPLeaseStateActive), now).
		Order("expires_at DESC, id DESC").Take(&row).Error; err != nil {
		return egress.IPLease{}, mapError(err)
	}
	return toEgressIPLeaseDomain(row), nil
}

// ObserveBuildEgressIPv4 records an IPv4 observed through a Build account's
// rendered sticky proxy URL. It intentionally accepts only a healthy result.
func (r *EgressRepository) ObserveBuildEgressIPv4(ctx context.Context, nodeID uint64, value egress.ProbeResult) (uint64, error) {
	if nodeID == 0 || value.Status != egress.ProbeStatusHealthy {
		return 0, repository.ErrInvalidEgressIPLease
	}
	observations := probeIPObservations(value)
	var observed probeIPObservation
	found := false
	for _, candidate := range observations {
		if candidate.family == "ipv4" {
			observed, found = candidate, true
			break
		}
	}
	if !found {
		return 0, repository.ErrInvalidEgressIPLease
	}

	var id uint64
	err := r.db.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var node egressNodeModel
		if err := tx.Select("scope").First(&node, nodeID).Error; err != nil {
			return mapError(err)
		}
		if node.Scope != string(egress.ScopeBuild) {
			return repository.ErrInvalidEgressIPLease
		}
		if err := upsertEgressIPObservations(tx, node.Scope, nodeID, value); err != nil {
			return err
		}
		var record egressIPRecordModel
		if err := tx.Model(&egressIPRecordModel{}).
			Where("scope = ? AND family = ? AND exit_ip = ?", node.Scope, observed.family, observed.ip).
			Select("id").Take(&record).Error; err != nil {
			return mapError(err)
		}
		id = record.ID
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// AcquireBuildEgressIPLease returns the account's existing unexpired Build
// lease when present. Otherwise it atomically reserves one slot on a verified
// IPv4 record and creates an active lease. The conditional counter update is
// the capacity authority across processes and database instances.
func (r *EgressRepository) AcquireBuildEgressIPLease(ctx context.Context, input egress.IPLeaseAcquireInput) (egress.IPLease, bool, error) {
	if input.IPRecordID == 0 || input.AccountID == 0 || input.MaxAccounts < 1 || input.ExpiresAt.IsZero() {
		return egress.IPLease{}, false, repository.ErrInvalidEgressIPLease
	}
	now := input.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expiresAt := input.ExpiresAt.UTC()
	if !expiresAt.After(now) {
		return egress.IPLease{}, false, repository.ErrInvalidEgressIPLease
	}

	var lease egress.IPLease
	created := false
	err := r.db.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := expireAccountBuildLeases(tx, input.AccountID, now); err != nil {
			return err
		}
		var existing egressIPLeaseModel
		err := tx.Where("account_id = ? AND scope = ? AND state = ? AND expires_at > ?", input.AccountID, string(egress.ScopeBuild), string(egress.IPLeaseStateActive), now).
			Clauses(clause.Locking{Strength: "UPDATE"}).Take(&existing).Error
		if err == nil {
			lease = toEgressIPLeaseDomain(existing)
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return mapError(err)
		}

		if err := expireRecordBuildLeases(tx, input.IPRecordID, now); err != nil {
			return err
		}
		result := tx.Model(&egressIPRecordModel{}).
			Where("id = ? AND scope = ? AND family = ? AND active_lease_count < ?", input.IPRecordID, string(egress.ScopeBuild), "ipv4", input.MaxAccounts).
			Updates(map[string]any{"active_lease_count": gorm.Expr("active_lease_count + 1"), "updated_at": now})
		if result.Error != nil {
			return mapError(result.Error)
		}
		if result.RowsAffected == 0 {
			var record egressIPRecordModel
			if err := tx.Select("id", "scope", "family").First(&record, input.IPRecordID).Error; err != nil {
				return mapError(err)
			}
			if record.Scope != string(egress.ScopeBuild) || record.Family != "ipv4" {
				return repository.ErrInvalidEgressIPLease
			}
			return repository.ErrEgressIPCapacity
		}
		var record egressIPRecordModel
		if err := tx.Select("last_probe_at").First(&record, input.IPRecordID).Error; err != nil {
			return mapError(err)
		}
		lastVerifiedAt := record.LastProbeAt

		row := egressIPLeaseModel{
			IPRecordID: input.IPRecordID, AccountID: input.AccountID, Scope: string(egress.ScopeBuild), EgressNodeID: input.EgressNodeID,
			State: string(egress.IPLeaseStateActive), AcquiredAt: now, RenewedAt: now, ExpiresAt: expiresAt,
			LastVerifiedAt: &lastVerifiedAt,
		}
		if err := tx.Create(&row).Error; err != nil {
			return mapError(err)
		}
		lease = toEgressIPLeaseDomain(row)
		created = true
		return nil
	})
	if err != nil {
		return egress.IPLease{}, false, err
	}
	return lease, created, nil
}

func (r *EgressRepository) ReleaseEgressIPLease(ctx context.Context, id uint64, reason string, releasedAt time.Time) error {
	if id == 0 || strings.TrimSpace(reason) == "" || len(reason) > 128 {
		return repository.ErrInvalidEgressIPLease
	}
	releasedAt = releasedAt.UTC()
	if releasedAt.IsZero() {
		releasedAt = time.Now().UTC()
	}
	return r.db.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var lease egressIPLeaseModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&lease, id).Error; err != nil {
			return mapError(err)
		}
		if lease.State != string(egress.IPLeaseStateActive) {
			return nil
		}
		result := tx.Model(&egressIPLeaseModel{}).Where("id = ? AND state = ?", id, string(egress.IPLeaseStateActive)).Updates(map[string]any{
			"state": string(egress.IPLeaseStateReleased), "released_at": releasedAt, "release_reason": reason, "updated_at": releasedAt,
		})
		if result.Error != nil {
			return mapError(result.Error)
		}
		if result.RowsAffected == 0 {
			return nil
		}
		return decrementEgressIPRecordLeaseCount(tx, lease.IPRecordID, releasedAt)
	})
}

func expireAccountBuildLeases(tx *gorm.DB, accountID uint64, now time.Time) error {
	var rows []egressIPLeaseModel
	if err := tx.Select("id", "ip_record_id").
		Where("account_id = ? AND scope = ? AND state = ? AND expires_at <= ?", accountID, string(egress.ScopeBuild), string(egress.IPLeaseStateActive), now).
		Clauses(clause.Locking{Strength: "UPDATE"}).Find(&rows).Error; err != nil {
		return mapError(err)
	}
	return expireEgressIPLeaseRows(tx, rows, now)
}

func expireRecordBuildLeases(tx *gorm.DB, recordID uint64, now time.Time) error {
	var rows []egressIPLeaseModel
	if err := tx.Select("id", "ip_record_id").
		Where("ip_record_id = ? AND scope = ? AND state = ? AND expires_at <= ?", recordID, string(egress.ScopeBuild), string(egress.IPLeaseStateActive), now).
		Clauses(clause.Locking{Strength: "UPDATE"}).Find(&rows).Error; err != nil {
		return mapError(err)
	}
	return expireEgressIPLeaseRows(tx, rows, now)
}

func expireEgressIPLeaseRows(tx *gorm.DB, rows []egressIPLeaseModel, now time.Time) error {
	for _, row := range rows {
		result := tx.Model(&egressIPLeaseModel{}).Where("id = ? AND state = ?", row.ID, string(egress.IPLeaseStateActive)).Updates(map[string]any{
			"state": string(egress.IPLeaseStateExpired), "released_at": now, "release_reason": "lease expired", "updated_at": now,
		})
		if result.Error != nil {
			return mapError(result.Error)
		}
		if result.RowsAffected == 1 {
			if err := decrementEgressIPRecordLeaseCount(tx, row.IPRecordID, now); err != nil {
				return err
			}
		}
	}
	return nil
}

func decrementEgressIPRecordLeaseCount(tx *gorm.DB, recordID uint64, at time.Time) error {
	result := tx.Model(&egressIPRecordModel{}).Where("id = ?", recordID).Updates(map[string]any{
		"active_lease_count": gorm.Expr("CASE WHEN active_lease_count > 0 THEN active_lease_count - 1 ELSE 0 END"),
		"updated_at":         at,
	})
	if result.Error != nil {
		return mapError(result.Error)
	}
	if result.RowsAffected == 0 {
		return repository.ErrNotFound
	}
	return nil
}

func toEgressIPLeaseDomain(row egressIPLeaseModel) egress.IPLease {
	return egress.IPLease{
		ID: row.ID, IPRecordID: row.IPRecordID, AccountID: row.AccountID, Scope: egress.Scope(row.Scope), EgressNodeID: row.EgressNodeID,
		State: egress.IPLeaseState(row.State), AcquiredAt: row.AcquiredAt, RenewedAt: row.RenewedAt, ExpiresAt: row.ExpiresAt,
		ReleasedAt: row.ReleasedAt, LastVerifiedAt: row.LastVerifiedAt, LastError: row.LastError, ReleaseReason: row.ReleaseReason,
		CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
	}
}
