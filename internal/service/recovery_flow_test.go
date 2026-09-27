package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/route"
)

func persistentControl(runID string, state model.RunState, withPermit bool) model.ControlSnapshot {
	now := time.Now().UTC()
	control := model.ControlSnapshot{
		Revision:   "persisted-" + runID,
		CapturedAt: now,
		Stations:   testStations(),
		Runs:       []model.Run{{ID: runID, Station: "station-a", State: state, Epoch: 2, CreatedAt: now, UpdatedAt: now}},
		Routes: []model.RouteState{{
			ID: "main-station-a-station-c", FromStation: "station-a", ToStation: "station-c",
			RunID: runID, LeaseEpoch: 2, Reserved: true, UpdatedAt: now,
		}},
		Pressures: []model.PressureState{{
			StationID: "station-a", RunID: runID, Value: 5, Target: 5,
			Mode: model.CompressorHolding, ObservedAt: now,
		}},
	}
	if withPermit {
		control.Permits = []model.PermitState{{
			RunID: runID, Name: "repressurization", Generation: 3, Granted: true, Reason: "durable evidence", UpdatedAt: now,
		}}
	}
	return control
}

func configureWithSnapshot(t *testing.T, dir string, control model.ControlSnapshot, events ...model.Event) *Runtime {
	t.Helper()
	runtime := NewRuntime()
	if err := runtime.ConfigurePersistence(dir); err != nil {
		t.Fatal(err)
	}
	store := runtime.ensureControl().Persistent
	for _, event := range events {
		if err := store.Append(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.WriteSnapshot(control); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestDryRunRecoveryRequiresPersistence(t *testing.T) {
	runtime := NewRuntime()
	if _, err := runtime.DryRunRecovery(context.Background(), ""); !errors.Is(err, ErrRecoveryPersistenceNotConfigured) {
		t.Fatalf("expected persistence error, got %v", err)
	}
}

func TestDryRunRecoveryIsReadOnly(t *testing.T) {
	dir := t.TempDir()
	runtime := configureWithSnapshot(t, dir, persistentControl("run-dry", model.StateLaunched, false))

	before := runtime.Snapshot()
	report, err := runtime.DryRunRecovery(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if report.Mutated || report.Mode != "dry_run" {
		t.Fatalf("dry run must advertise read-only mode, got %+v", report)
	}
	if !report.Ready || report.BlockedStage != "" {
		t.Fatalf("expected recoverable snapshot, got %+v", report.Evidence.Stages)
	}
	if len(report.Evidence.Runs) != 1 || report.Evidence.Runs[0].ID != "run-dry" {
		t.Fatalf("expected the run listed as recoverable, got %+v", report.Evidence.Runs)
	}
	if len(report.Evidence.Routes) != 1 {
		t.Fatalf("expected the route lease listed as recoverable, got %+v", report.Evidence.Routes)
	}
	after := runtime.Snapshot()
	if after.Revision != before.Revision {
		t.Fatal("dry run changed live control revision")
	}
	if _, ok := runtime.Runs.Get("run-dry"); ok {
		t.Fatal("dry run must not populate the run store")
	}
	if len(report.SnapshotFile) == 0 || !strings.HasSuffix(report.SnapshotFile, "snapshot.json") {
		t.Fatalf("expected snapshot file path in report, got %q", report.SnapshotFile)
	}

	// A second call re-reads the same evidence from disk and stays read-only.
	again, err := runtime.DryRunRecovery(context.Background(), "snapshot.json")
	if err != nil || !again.Ready {
		t.Fatalf("repeat dry run failed: %v %+v", err, again)
	}
}

func TestDryRunRecoveryNamedSnapshotAndMissing(t *testing.T) {
	dir := t.TempDir()
	runtime := configureWithSnapshot(t, dir, persistentControl("run-dry", model.StateLaunched, false))

	named := filepath.Join(dir, "snapshot-old.json")
	payload, err := os.ReadFile(filepath.Join(dir, "snapshot.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(named, payload, 0o640); err != nil {
		t.Fatal(err)
	}
	report, err := runtime.DryRunRecovery(context.Background(), "snapshot-old.json")
	if err != nil || !report.Ready {
		t.Fatalf("named snapshot dry run failed: %v %+v", err, report)
	}

	_, err = runtime.DryRunRecovery(context.Background(), "snapshot-missing.json")
	var snapshotErr *SnapshotError
	if !errors.As(err, &snapshotErr) || snapshotErr.Kind != SnapshotNotFound {
		t.Fatalf("expected not-found snapshot error, got %v", err)
	}

	if _, err := runtime.DryRunRecovery(context.Background(), "../escape.json"); err == nil {
		t.Fatal("expected path traversal to be rejected")
	}
}

func TestDryRunReportsBlockedEvidence(t *testing.T) {
	dir := t.TempDir()
	// Snapshot tail points at one event ...
	passage := model.Event{RunID: "run-block", Kind: "passage", Epoch: 3, At: time.Now().UTC()}
	runtime := configureWithSnapshot(t, dir, persistentControl("run-block", model.StateTracking, true), passage)
	// ... then the journal is removed, so evidence no longer covers it.
	if err := os.Remove(filepath.Join(dir, "events.jsonl")); err != nil {
		t.Fatal(err)
	}

	report, err := runtime.DryRunRecovery(context.Background(), "")
	if err != nil {
		t.Fatalf("blocked evidence is a verdict, not an error: %v", err)
	}
	if report.Ready || report.BlockedStage != RecoveryJournal {
		t.Fatalf("expected block at journal stage, got ready=%v stage=%q", report.Ready, report.BlockedStage)
	}
	journalFinding := stageFinding(report.Evidence, RecoveryJournal)
	if len(journalFinding.BlockingReasons) == 0 {
		t.Fatal("expected journal blocking reasons")
	}

	// The dry-run verdict must not have changed any live state.
	if _, ok := runtime.Runs.Get("run-block"); ok {
		t.Fatal("blocked dry run must not publish the run")
	}
}

func TestRecoverPersistentRestoresCompleteState(t *testing.T) {
	dir := t.TempDir()
	passage := model.Event{RunID: "run-ok", Kind: "passage", Epoch: 3, At: time.Now().UTC()}
	runtime := configureWithSnapshot(t, dir, persistentControl("run-ok", model.StateTracking, true), passage)

	// A dry run beforehand must not satisfy recovery: formal recovery reads the
	// evidence itself.
	if _, err := runtime.DryRunRecovery(context.Background(), ""); err != nil {
		t.Fatal(err)
	}

	report, err := runtime.RecoverPersistent()
	if err != nil {
		t.Fatalf("clean recovery failed: %v", err)
	}
	if !report.Ready || report.RecoveredRuns != 1 || report.RecoveredRoutes != 1 || report.RecoveredPermits != 1 {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	recovered, ok := runtime.Runs.Get("run-ok")
	if !ok || recovered.State != model.StateTracking {
		t.Fatalf("run not restored: %+v", recovered)
	}
	leases := route.Snapshot(runtime.Routes)
	if len(leases) != 1 || leases[0].RunID != "run-ok" {
		t.Fatalf("route lease not restored: %+v", leases)
	}
	if !runtime.Permits.IsOpen("run-ok") {
		t.Fatal("permit not restored")
	}
	live := runtime.Snapshot()
	var reserved int
	for _, routeState := range live.Routes {
		if routeState.Reserved && routeState.RunID == "run-ok" {
			reserved++
		}
	}
	if reserved != 1 {
		t.Fatalf("control routes not restored, got %+v", live.Routes)
	}
	if err := runtime.RecoveryBarrier(); err != nil {
		t.Fatalf("recovered runtime must pass the barrier: %v", err)
	}
}

func TestRecoverPersistentBlockedPublishesNothing(t *testing.T) {
	dir := t.TempDir()
	passage := model.Event{RunID: "run-block", Kind: "passage", Epoch: 3, At: time.Now().UTC()}
	runtime := configureWithSnapshot(t, dir, persistentControl("run-block", model.StateTracking, true), passage)

	// Seed pre-existing live state that must survive the failed recovery.
	seed := runtime.Runs.Create("station-a")

	// Corrupt the journal after the snapshot was written: formal recovery must
	// re-read and refuse rather than trust any earlier verdict.
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), []byte("{\"sequence\":1}\n"), 0o640); err != nil {
		t.Fatal(err)
	}

	report, err := runtime.RecoverPersistent()
	if err == nil {
		t.Fatal("expected recovery to fail on the corrupted journal")
	}
	if report.Ready {
		t.Fatalf("failed recovery must not report ready: %+v", report)
	}
	if _, ok := runtime.Runs.Get("run-block"); ok {
		t.Fatal("blocked recovery must not publish the snapshot run")
	}
	if _, ok := runtime.Runs.Get(seed.ID); !ok {
		t.Fatal("blocked recovery destroyed the pre-existing run")
	}
	if len(route.Snapshot(runtime.Routes)) != 0 {
		t.Fatal("blocked recovery must not publish route leases")
	}
	if runtime.Permits.IsOpen("run-block") {
		t.Fatal("blocked recovery must not publish permits")
	}
}

func TestRecoverPersistentEvidenceBlockedPublishesNothing(t *testing.T) {
	dir := t.TempDir()
	// Snapshot references one event but the log is gone: evidence analysis
	// blocks at the journal stage before any state swap.
	runtime := configureWithSnapshot(t, dir, persistentControl("run-block", model.StateLaunched, false),
		model.Event{RunID: "run-block", Kind: "loaded", Epoch: 2, At: time.Now().UTC()})
	if err := os.Remove(filepath.Join(dir, "events.jsonl")); err != nil {
		t.Fatal(err)
	}
	seed := runtime.Runs.Create("station-a")

	if _, err := runtime.RecoverPersistent(); err == nil {
		t.Fatal("expected evidence-blocked recovery to fail")
	}
	if _, ok := runtime.Runs.Get("run-block"); ok {
		t.Fatal("evidence-blocked recovery must not publish the snapshot run")
	}
	if _, ok := runtime.Runs.Get(seed.ID); !ok {
		t.Fatal("evidence-blocked recovery destroyed the pre-existing run")
	}
}
