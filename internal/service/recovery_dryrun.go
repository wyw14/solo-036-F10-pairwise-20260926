package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

// ErrRecoveryPersistenceNotConfigured is returned when no journal directory is
// configured, so recovery (or a dry run of it) has no evidence to read.
var ErrRecoveryPersistenceNotConfigured = errors.New("persistent journal is not configured")

// SnapshotErrorKind classifies a snapshot access failure.
type SnapshotErrorKind string

const (
	// SnapshotNotFound means the referenced snapshot file does not exist.
	SnapshotNotFound SnapshotErrorKind = "not_found"
	// SnapshotInvalid means the snapshot exists but cannot be decoded or
	// fails structural validation.
	SnapshotInvalid SnapshotErrorKind = "invalid"
	// JournalInvalid means the event log backing the snapshot cannot be read
	// or its checksum chain does not verify.
	JournalInvalid SnapshotErrorKind = "journal_invalid"
)

// SnapshotError describes why a snapshot or its backing journal could not be
// examined read-only. The API layer maps the kind to an HTTP status.
type SnapshotError struct {
	Path string
	Kind SnapshotErrorKind
	Err  error
}

func (e *SnapshotError) Error() string { return e.Err.Error() }
func (e *SnapshotError) Unwrap() error { return e.Err }

// RecoveryDryRunReport is the read-only verdict for a snapshot. It contains no
// live runtime state: every figure is derived from the snapshot file and
// event journal read fresh from disk during the call.
type RecoveryDryRunReport struct {
	Mode              string                 `json:"mode"`
	Mutated           bool                   `json:"mutated"`
	SnapshotFile      string                 `json:"snapshot_file"`
	SnapshotWrittenAt time.Time              `json:"snapshot_written_at,omitempty"`
	Ready             bool                   `json:"ready"`
	BlockedStage      RecoveryStage          `json:"blocked_stage,omitempty"`
	Evidence          RecoveryEvidenceReport `json:"evidence"`
}

// DryRunRecovery examines a snapshot without modifying any runtime state. The
// reference may be empty (canonical snapshot.json) or a relative path inside
// the configured journal directory. The snapshot and the full event journal
// are re-read from disk for every call; conclusions from a dry run are never
// reused by RecoverPersistent, which reads the same evidence again.
func (r *Runtime) DryRunRecovery(ctx context.Context, snapshotReference string) (RecoveryDryRunReport, error) {
	if err := ctx.Err(); err != nil {
		return RecoveryDryRunReport{}, err
	}
	if err := ValidateRecoveryOrdering(); err != nil {
		return RecoveryDryRunReport{}, err
	}
	control := r.ensureControl()
	r.mu.Lock()
	persistent := control.Persistent
	r.mu.Unlock()
	if persistent == nil {
		return RecoveryDryRunReport{}, ErrRecoveryPersistenceNotConfigured
	}

	path, err := persistent.ResolveSnapshotPath(snapshotReference)
	if err != nil {
		return RecoveryDryRunReport{}, &SnapshotError{Path: snapshotReference, Kind: SnapshotInvalid, Err: err}
	}
	snapshot, err := persistent.DecodeSnapshot(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return RecoveryDryRunReport{}, &SnapshotError{Path: path, Kind: SnapshotNotFound, Err: err}
		}
		return RecoveryDryRunReport{}, &SnapshotError{Path: path, Kind: SnapshotInvalid, Err: err}
	}
	if err := ctx.Err(); err != nil {
		return RecoveryDryRunReport{}, err
	}
	events, err := persistent.Events()
	if err != nil {
		return RecoveryDryRunReport{}, &SnapshotError{
			Path: path,
			Kind: JournalInvalid,
			Err:  fmt.Errorf("read event journal for %s: %w", path, err),
		}
	}

	evidence := analyzeRecoveryEvidence(snapshot, events)
	report := RecoveryDryRunReport{
		Mode:              "dry_run",
		Mutated:           false,
		SnapshotFile:      path,
		SnapshotWrittenAt: snapshot.WrittenAt,
		Ready:             evidence.Ready(),
		Evidence:          evidence,
	}
	if stage, blocked := evidence.BlockedAt(); blocked {
		report.BlockedStage = stage
	}
	return report, nil
}
