package relational

import (
	"context"
	"errors"
	"fmt"
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
	var record egressIPRecordModel
	if err := r.db.db.WithContext(ctx).Select("exit_ip").First(&record, row.IPRecordID).Error; err != nil {
		return egress.IPLease{}, mapError(err)
	}
	lease := toEgressIPLeaseDomain(row)
	lease.ExitIP = record.ExitIP
	return lease, nil
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
		if err := tx.Select("last_probe_at", "exit_ip").First(&record, input.IPRecordID).Error; err != nil {
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
		lease.ExitIP = record.ExitIP
		created = true
		return nil
	})
	if err != nil {
		return egress.IPLease{}, false, err
	}
	return lease, created, nil
}

// RenewBuildEgressIPLease records a successful account-specific IP
// verification. It does not change the IP binding or consume another slot.
func (r *EgressRepository) RenewBuildEgressIPLease(ctx context.Context, id uint64, verifiedAt, expiresAt time.Time) (egress.IPLease, error) {
	if id == 0 {
		return egress.IPLease{}, repository.ErrInvalidEgressIPLease
	}
	verifiedAt = verifiedAt.UTC()
	if verifiedAt.IsZero() {
		verifiedAt = time.Now().UTC()
	}
	expiresAt = expiresAt.UTC()
	if !expiresAt.After(verifiedAt) {
		return egress.IPLease{}, repository.ErrInvalidEgressIPLease
	}

	var lease egress.IPLease
	err := r.db.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row egressIPLeaseModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, id).Error; err != nil {
			return mapError(err)
		}
		if row.Scope != string(egress.ScopeBuild) || row.State != string(egress.IPLeaseStateActive) || !row.ExpiresAt.After(verifiedAt) {
			return repository.ErrInvalidEgressIPLease
		}
		result := tx.Model(&egressIPLeaseModel{}).Where("id = ? AND state = ?", id, string(egress.IPLeaseStateActive)).Updates(map[string]any{
			"last_verified_at": verifiedAt, "renewed_at": verifiedAt, "expires_at": expiresAt, "last_error": "", "updated_at": verifiedAt,
		})
		if result.Error != nil {
			return mapError(result.Error)
		}
		if result.RowsAffected != 1 {
			return repository.ErrInvalidEgressIPLease
		}
		row.LastVerifiedAt, row.RenewedAt, row.ExpiresAt, row.LastError, row.UpdatedAt = &verifiedAt, verifiedAt, expiresAt, "", verifiedAt
		var record egressIPRecordModel
		if err := tx.Select("exit_ip").First(&record, row.IPRecordID).Error; err != nil {
			return mapError(err)
		}
		lease = toEgressIPLeaseDomain(row)
		lease.ExitIP = record.ExitIP
		return nil
	})
	if err != nil {
		return egress.IPLease{}, err
	}
	return lease, nil
}

// ObserveBuildEgressRequest records only attribution and aggregate load. It
// deliberately does not change account or IP eligibility.
func (r *EgressRepository) ObserveBuildEgressRequest(ctx context.Context, input egress.BuildRequestObservation) error {
	if input.LeaseID == 0 || input.AccountID == 0 || input.BotFlagSource < 0 || input.BotFlagSource > 2 || input.StatusCode < 0 || input.StatusCode > 999 {
		return repository.ErrInvalidEgressIPLease
	}
	observedAt := input.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	windowStartedAt := observedAt.Truncate(5 * time.Minute)
	return r.db.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var lease egressIPLeaseModel
		if err := tx.Select("id", "ip_record_id", "account_id", "scope").First(&lease, input.LeaseID).Error; err != nil {
			return mapError(err)
		}
		if lease.Scope != string(egress.ScopeBuild) || lease.AccountID != input.AccountID {
			return repository.ErrInvalidEgressIPLease
		}
		risk := buildAccountRiskObservationModel{
			EgressIPLeaseID: input.LeaseID, IPRecordID: lease.IPRecordID, AccountID: input.AccountID,
			BotFlagSource: input.BotFlagSource, ObservedAt: observedAt,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "egress_ip_lease_id"}},
			DoUpdates: clause.Assignments(map[string]any{"ip_record_id": lease.IPRecordID, "account_id": input.AccountID, "bot_flag_source": input.BotFlagSource, "observed_at": observedAt, "updated_at": observedAt}),
		}).Create(&risk).Error; err != nil {
			return mapError(err)
		}
		usage := buildEgressUsageWindowModel{
			IPRecordID: lease.IPRecordID, AccountID: input.AccountID, WindowStartedAt: windowStartedAt,
			RequestCount: 1, LastStatusCode: input.StatusCode, FirstObservedAt: observedAt, LastObservedAt: observedAt,
		}
		return mapError(tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "ip_record_id"}, {Name: "account_id"}, {Name: "window_started_at"}},
			DoUpdates: clause.Assignments(map[string]any{"request_count": gorm.Expr("request_count + 1"), "last_status_code": input.StatusCode, "last_observed_at": observedAt, "updated_at": observedAt}),
		}).Create(&usage).Error)
	})
}

func (r *EgressRepository) ObserveBuildEgressOutcome(ctx context.Context, input egress.BuildResponseOutcome) error {
	if input.LeaseID == 0 || input.AccountID == 0 || strings.TrimSpace(input.Model) == "" || len(input.Model) > 160 || input.StatusCode < 200 || input.StatusCode >= 300 {
		return repository.ErrInvalidEgressIPLease
	}
	at := input.ObservedAt.UTC()
	if at.IsZero() {
		at = time.Now().UTC()
	}
	return r.db.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var lease egressIPLeaseModel
		if err := tx.Select("ip_record_id", "account_id", "scope").First(&lease, input.LeaseID).Error; err != nil {
			return mapError(err)
		}
		if lease.Scope != string(egress.ScopeBuild) || lease.AccountID != input.AccountID {
			return repository.ErrInvalidEgressIPLease
		}
		outcome := buildEgressOutcomeWindowModel{IPRecordID: lease.IPRecordID, AccountID: input.AccountID, Model: strings.TrimSpace(input.Model), WindowStartedAt: at.Truncate(5 * time.Minute), CompletedResponseCount: 1, LastStatusCode: input.StatusCode, LastObservedAt: at}
		if input.ReasoningObserved {
			outcome.ReasoningObservedCount = 1
		} else {
			outcome.MissingReasoningCount = 1
		}
		return mapError(tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "ip_record_id"}, {Name: "account_id"}, {Name: "model"}, {Name: "window_started_at"}}, DoUpdates: clause.Assignments(map[string]any{"completed_response_count": gorm.Expr("completed_response_count + 1"), "reasoning_observed_count": gorm.Expr("reasoning_observed_count + ?", outcome.ReasoningObservedCount), "missing_reasoning_count": gorm.Expr("missing_reasoning_count + ?", outcome.MissingReasoningCount), "last_status_code": input.StatusCode, "last_observed_at": at, "updated_at": at})}).Create(&outcome).Error)
	})
}

func (r *EgressRepository) ListBuildEgressRiskSummaries(ctx context.Context, since time.Time, limit int) ([]egress.BuildEgressRiskSummary, error) {
	if limit < 1 || limit > 500 {
		return nil, repository.ErrInvalidEgressIPLease
	}
	since = since.UTC()
	if since.IsZero() {
		since = time.Now().UTC().Add(-10 * time.Minute)
	}
	type row struct {
		ID               uint64
		ExitIP           string
		ActiveLeaseCount uint64
		LastSeenAt       time.Time
	}
	var records []row
	if err := r.db.db.WithContext(ctx).Model(&egressIPRecordModel{}).Select("id, exit_ip, active_lease_count, last_seen_at").Where("scope = ? AND family = ?", string(egress.ScopeBuild), "ipv4").Order("last_seen_at DESC, id DESC").Limit(limit).Scan(&records).Error; err != nil {
		return nil, mapError(err)
	}
	result := make([]egress.BuildEgressRiskSummary, 0, len(records))
	for _, record := range records {
		var usage struct {
			Accounts uint64
			Requests uint64
			Last     egressRiskTimestamp
		}
		if err := r.db.db.WithContext(ctx).Model(&buildEgressUsageWindowModel{}).Select("COUNT(DISTINCT account_id) AS accounts, COALESCE(SUM(request_count), 0) AS requests, MAX(last_observed_at) AS last").Where("ip_record_id = ? AND last_observed_at >= ?", record.ID, since).Scan(&usage).Error; err != nil {
			return nil, mapError(err)
		}
		var flags struct {
			One uint64
			Two uint64
		}
		if err := r.db.db.WithContext(ctx).Model(&buildAccountRiskObservationModel{}).Select("COUNT(DISTINCT CASE WHEN bot_flag_source = 1 THEN account_id END) AS one, COUNT(DISTINCT CASE WHEN bot_flag_source = 2 THEN account_id END) AS two").Where("ip_record_id = ? AND observed_at >= ?", record.ID, since).Scan(&flags).Error; err != nil {
			return nil, mapError(err)
		}
		var outcomes struct {
			Completed uint64
			Reasoning uint64
			Missing   uint64
			Last      egressRiskTimestamp
		}
		if err := r.db.db.WithContext(ctx).Model(&buildEgressOutcomeWindowModel{}).Select("COALESCE(SUM(completed_response_count), 0) AS completed, COALESCE(SUM(reasoning_observed_count), 0) AS reasoning, COALESCE(SUM(missing_reasoning_count), 0) AS missing, MAX(last_observed_at) AS last").Where("ip_record_id = ? AND last_observed_at >= ?", record.ID, since).Scan(&outcomes).Error; err != nil {
			return nil, mapError(err)
		}
		state := "unknown"
		if flags.Two >= 2 {
			state = "ip_farm_suspected"
		} else if flags.One > 0 || flags.Two > 0 {
			state = "account_flagged"
		} else if usage.Requests > 0 {
			state = "observed"
		}
		last := time.Time(usage.Last)
		if outcomeLast := time.Time(outcomes.Last); outcomeLast.After(last) {
			last = outcomeLast
		}
		result = append(result, egress.BuildEgressRiskSummary{IPRecordID: record.ID, ExitIP: record.ExitIP, ActiveLeaseCount: record.ActiveLeaseCount, WindowAccountCount: usage.Accounts, WindowRequestCount: usage.Requests, BotFlagOneAccountCount: flags.One, BotFlagTwoAccountCount: flags.Two, CompletedResponseCount: outcomes.Completed, ReasoningObservedCount: outcomes.Reasoning, MissingReasoningCount: outcomes.Missing, RiskState: state, WindowStartedAt: since, LastObservedAt: last})
	}
	return result, nil
}

// egressRiskTimestamp accepts the SQLite text result returned by MAX(datetime)
// while keeping empty observation windows as a zero time.
type egressRiskTimestamp time.Time

func (value *egressRiskTimestamp) Scan(input any) error {
	parsed, err := parseEgressRiskTimestamp(input)
	if err != nil {
		return err
	}
	*value = egressRiskTimestamp(parsed)
	return nil
}

func parseEgressRiskTimestamp(input any) (time.Time, error) {
	if parsed, ok := input.(time.Time); ok {
		return parsed, nil
	}
	var raw string
	switch value := input.(type) {
	case string:
		raw = strings.TrimSpace(value)
	case []byte:
		raw = strings.TrimSpace(string(value))
	case nil:
		return time.Time{}, nil
	default:
		return time.Time{}, fmt.Errorf("unsupported egress risk timestamp type %T", input)
	}
	if raw == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05",
	} {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid egress risk timestamp")
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
