package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/journal"
	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/route"
)

func baseRecoverySnapshot() model.ControlSnapshot {
	return model.ControlSnapshot{
		Revision:   "rev-test",
		CapturedAt: time.Now().UTC(),
		Stations: []model.Station{
			{ID: "station-a", Name: "Alpha", Role: model.StationLauncher, Kilometer: 0, PressureLimit: 100, Enabled: true},
			{ID: "station-b", Name: "Beta", Role: model.StationReceiver, Kilometer: 10, PressureLimit: 100, Enabled: true},
		},
		Routes: []model.RouteState{
			{ID: "route-ab", FromStation: "station-a", ToStation: "station-b"},
		},
	}
}

func writeSnapshot(t *testing.T, dir string, snapshot model.ControlSnapshot) {
	t.Helper()
	store, err := journal.NewFileStore(dir)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	if err := store.WriteSnapshot(snapshot); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
}

func persistentRuntime(t *testing.T, dir string) *Runtime {
	t.Helper()
	rt := NewRuntime()
	if err := rt.ConfigurePersistence(dir); err != nil {
		t.Fatalf("configure persistence: %v", err)
	}
	return rt
}

func requireStage(t *testing.T, report RecoveryDryRun, stage RecoveryStage) RecoveryStageStatus {
	t.Helper()
	for _, status := range report.Stages {
		if status.Stage == stage {
			return status
		}
	}
	t.Fatalf("stage %s missing from dry run report", stage)
	return RecoveryStageStatus{}
}

func TestDryRunRecoveryReadySnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := journal.NewFileStore(dir)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	if err := store.Append(model.Event{RunID: "run-1", Kind: "launch", Epoch: 2}); err != nil {
		t.Fatalf("append event: %v", err)
	}
	snapshot := baseRecoverySnapshot()
	snapshot.Runs = []model.Run{{ID: "run-1", Station: "station-a", State: model.StateTracking, Epoch: 3}}
	snapshot.Routes[0].Reserved = true
	snapshot.Routes[0].RunID = "run-1"
	snapshot.Routes[0].LeaseEpoch = 2
	snapshot.Permits = []model.PermitState{{RunID: "run-1", Name: "repressurization", Generation: 3, Granted: true}}
	if err := store.WriteSnapshot(snapshot); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	rt := persistentRuntime(t, dir)

	report := rt.DryRunRecovery()
	if !report.Ready {
		t.Fatalf("dry run not ready, blocked at %s: %s", report.BlockedAt, report.Blocker())
	}
	if !report.Persisted {
		t.Error("expected a persisted report")
	}
	if report.BlockedAt != "" {
		t.Errorf("unexpected blocked stage %s", report.BlockedAt)
	}
	if len(report.Stages) != len(canonicalRecoveryOrder) {
		t.Fatalf("got %d stages, want %d", len(report.Stages), len(canonicalRecoveryOrder))
	}
	for _, stage := range report.Stages {
		if !stage.Ready {
			t.Errorf("stage %s blocked: %s", stage.Stage, stage.Blocked)
		}
	}
	if report.Snapshot == nil || report.Snapshot.LastSequence != 1 {
		t.Errorf("snapshot summary missing or wrong sequence: %+v", report.Snapshot)
	}
	if report.JournalEvents != 1 {
		t.Errorf("got %d journal events, want 1", report.JournalEvents)
	}
	if len(report.Runs) != 1 || !report.Runs[0].Recoverable {
		t.Errorf("runs not recoverable: %+v", report.Runs)
	}
	if len(report.Routes) != 1 || !report.Routes[0].Recoverable || report.Routes[0].RunID != "run-1" {
		t.Errorf("routes not recoverable: %+v", report.Routes)
	}
	if len(report.Permits) != 1 || !report.Permits[0].Recoverable {
		t.Errorf("permits not recoverable: %+v", report.Permits)
	}
	if got := len(rt.Runs.Snapshot()); got != 0 {
		t.Fatalf("dry run mutated run store: %d runs", got)
	}
	if got := len(route.Snapshot(rt.Routes)); got != 0 {
		t.Fatalf("dry run mutated route leases: %d", got)
	}
	if rt.Permits.IsOpen("run-1") {
		t.Fatal("dry run mutated permits")
	}

	// The formal recovery must re-read the evidence: replace the snapshot
	// after the dry run and expect the new contents to be restored.
	updated := snapshot
	updated.Runs = append(append([]model.Run(nil), snapshot.Runs...),
		model.Run{ID: "run-2", Station: "station-a", State: model.StateLaunched, Epoch: 2})
	writeSnapshot(t, dir, updated)
	recovered, err := rt.RecoverPersistent()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if !recovered.Ready {
		t.Error("recovery report not ready")
	}
	if recovered.RecoveredRuns != 2 || recovered.RecoveredRoutes != 1 || recovered.RecoveredPermits != 1 {
		t.Errorf("unexpected recovery counts: %+v", recovered)
	}
	if recovered.JournalEvents != 1 {
		t.Errorf("got %d journal events, want 1", recovered.JournalEvents)
	}
	if _, ok := rt.Runs.Get("run-2"); !ok {
		t.Error("run-2 from the refreshed snapshot was not restored")
	}
	leases := route.Snapshot(rt.Routes)
	if len(leases) != 1 || leases[0].RunID != "run-1" || leases[0].Epoch != 2 {
		t.Errorf("unexpected leases: %+v", leases)
	}
	if !rt.Permits.IsOpen("run-1") {
		t.Error("permit for run-1 was not restored")
	}
	if got := len(rt.Snapshot().Runs); got != 2 {
		t.Errorf("control snapshot has %d runs, want 2", got)
	}
}

func TestDryRunRecoveryBlockedItems(t *testing.T) {
	dir := t.TempDir()
	snapshot := baseRecoverySnapshot()
	snapshot.Runs = []model.Run{
		{ID: "run-bad", State: model.StateTracking, Epoch: 1},
		{ID: "run-done", Station: "station-a", State: model.StateComplete, Epoch: 4},
	}
	snapshot.Routes[0].Reserved = true
	snapshot.Routes[0].RunID = "run-done"
	snapshot.Routes[0].LeaseEpoch = 2
	snapshot.Permits = []model.PermitState{{RunID: "run-ghost", Name: "repressurization", Generation: 2, Granted: true}}
	writeSnapshot(t, dir, snapshot)
	rt := persistentRuntime(t, dir)

	report := rt.DryRunRecovery()
	if report.Ready {
		t.Fatal("dry run should not be ready")
	}
	if report.BlockedAt != RecoveryRuns {
		t.Fatalf("blocked at %s, want %s", report.BlockedAt, RecoveryRuns)
	}
	if stage := requireStage(t, report, RecoveryJournal); !stage.Ready {
		t.Errorf("journal stage should be ready: %s", stage.Blocked)
	}
	if stage := requireStage(t, report, RecoveryRuns); stage.Ready || !strings.Contains(stage.Blocked, "run-bad") {
		t.Errorf("runs stage should be blocked by run-bad: %+v", stage)
	}
	if stage := requireStage(t, report, RecoveryRoutes); stage.Ready || !strings.Contains(stage.Blocked, string(RecoveryRuns)) {
		t.Errorf("routes stage should wait on runs: %+v", stage)
	}
	if stage := requireStage(t, report, RecoveryPressure); stage.Ready {
		t.Error("pressure stage should not be ready")
	}
	var badRun, doneRun *RunRecovery
	for i := range report.Runs {
		switch report.Runs[i].RunID {
		case "run-bad":
			badRun = &report.Runs[i]
		case "run-done":
			doneRun = &report.Runs[i]
		}
	}
	if badRun == nil || badRun.Recoverable || !strings.Contains(badRun.Blocked, "incomplete") {
		t.Errorf("run-bad should be blocked as incomplete: %+v", badRun)
	}
	if doneRun == nil || !doneRun.Recoverable {
		t.Errorf("run-done should be recoverable: %+v", doneRun)
	}
	if len(report.Routes) != 1 || report.Routes[0].Recoverable ||
		!strings.Contains(report.Routes[0].Blocked, "terminal run run-done") {
		t.Errorf("route should be blocked by its terminal owner: %+v", report.Routes)
	}
	if len(report.Permits) != 1 || report.Permits[0].Recoverable ||
		!strings.Contains(report.Permits[0].Blocked, "missing run run-ghost") {
		t.Errorf("permit should be blocked by its missing run: %+v", report.Permits)
	}

	// The formal recovery must fail at the same stage and publish nothing.
	_, err := rt.RecoverPersistent()
	if err == nil || !strings.Contains(err.Error(), string(RecoveryRuns)) {
		t.Fatalf("recovery should fail at the runs stage: %v", err)
	}
	if got := len(rt.Runs.Snapshot()); got != 0 {
		t.Errorf("half-published runs: %d", got)
	}
	if got := len(route.Snapshot(rt.Routes)); got != 0 {
		t.Errorf("half-published route leases: %d", got)
	}
	if rt.Permits.IsOpen("run-ghost") {
		t.Error("half-published permit")
	}
	if got := len(rt.Snapshot().Runs); got != 0 {
		t.Errorf("half-published control runs: %d", got)
	}
}

func TestDryRunRecoveryMissingSnapshot(t *testing.T) {
	rt := persistentRuntime(t, t.TempDir())

	report := rt.DryRunRecovery()
	if report.Ready {
		t.Fatal("dry run should not be ready without a snapshot")
	}
	if report.BlockedAt != RecoveryJournal {
		t.Fatalf("blocked at %s, want %s", report.BlockedAt, RecoveryJournal)
	}
	if stage := requireStage(t, report, RecoveryJournal); stage.Ready ||
		!strings.Contains(stage.Blocked, "snapshot") {
		t.Errorf("journal stage should report the missing snapshot: %+v", stage)
	}
	if stage := requireStage(t, report, RecoveryRuns); stage.Ready {
		t.Error("runs stage should wait on the journal stage")
	}
	if len(report.Runs) != 0 || len(report.Routes) != 0 || len(report.Permits) != 0 {
		t.Errorf("no items should be listed: %+v", report)
	}
	if _, err := rt.RecoverPersistent(); err == nil ||
		!strings.Contains(err.Error(), string(RecoveryJournal)) {
		t.Fatalf("recovery should fail at the journal stage: %v", err)
	}
	if got := len(rt.Runs.Snapshot()); got != 0 {
		t.Errorf("half-published runs: %d", got)
	}
}

func TestDryRunRecoveryCorruptJournal(t *testing.T) {
	dir := t.TempDir()
	rt := persistentRuntime(t, dir)
	writeSnapshot(t, dir, baseRecoverySnapshot())
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), []byte("not-json\n"), 0o640); err != nil {
		t.Fatalf("corrupt journal: %v", err)
	}

	report := rt.DryRunRecovery()
	if report.Ready || report.BlockedAt != RecoveryJournal {
		t.Fatalf("expected a journal-stage block, got %+v", report)
	}
	if _, err := rt.RecoverPersistent(); err == nil {
		t.Fatal("recovery should fail on a corrupt journal")
	}
	if got := len(rt.Runs.Snapshot()); got != 0 {
		t.Errorf("half-published runs: %d", got)
	}
}

func TestDryRunRecoveryWithoutPersistence(t *testing.T) {
	rt := NewRuntime()

	report := rt.DryRunRecovery()
	if !report.Ready {
		t.Fatalf("dry run not ready: %s", report.Blocker())
	}
	if report.Persisted {
		t.Error("report should not be marked persisted")
	}
	if len(report.Stages) != len(canonicalRecoveryOrder) {
		t.Fatalf("got %d stages, want %d", len(report.Stages), len(canonicalRecoveryOrder))
	}
	recovered, err := rt.RecoverPersistent()
	if err != nil || !recovered.Ready {
		t.Errorf("recovery without persistence should be a no-op: %+v, %v", recovered, err)
	}
}
