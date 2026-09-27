package service

import (
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/journal"
	"github.com/wyw14/solo-036/internal/model"
)

func testStations() []model.Station {
	return []model.Station{
		{ID: "station-a", Name: "North Launcher", Role: model.StationLauncher, Kilometer: 0, PressureLimit: 12.5, Enabled: true},
		{ID: "station-b", Name: "Central Block Valve", Role: model.StationIntermediate, Kilometer: 86.4, PressureLimit: 11.8, Enabled: true},
		{ID: "station-c", Name: "South Receiver", Role: model.StationReceiver, Kilometer: 173.2, PressureLimit: 11.2, Enabled: true},
	}
}

func testRun(id string, state model.RunState) model.Run {
	now := time.Now().UTC()
	return model.Run{ID: id, Station: "station-a", State: state, Epoch: 1, CreatedAt: now, UpdatedAt: now}
}

func testReservedRoute(runID string) model.RouteState {
	return model.RouteState{
		ID: "main-station-a-station-c", FromStation: "station-a", ToStation: "station-c",
		RunID: runID, LeaseEpoch: 1, Reserved: true, UpdatedAt: time.Now().UTC(),
	}
}

func baseControl() model.ControlSnapshot {
	return model.ControlSnapshot{
		Revision:   "test-revision",
		CapturedAt: time.Now().UTC(),
		Stations:   testStations(),
	}
}

func analyze(control model.ControlSnapshot, events ...model.Event) RecoveryEvidenceReport {
	return analyzeRecoveryEvidence(journal.FileSnapshot{Version: 1, LastSequence: int64(len(events)), Control: control}, events)
}

func stageFinding(report RecoveryEvidenceReport, stage RecoveryStage) RecoveryStageFinding {
	for _, finding := range report.Stages {
		if finding.Stage == stage {
			return finding
		}
	}
	return RecoveryStageFinding{Stage: stage, Recoverable: true}
}

func TestAnalyzeEvidenceCleanSnapshotIsReady(t *testing.T) {
	control := baseControl()
	control.Runs = []model.Run{testRun("run-1", model.StateLaunched)}
	control.Routes = []model.RouteState{testReservedRoute("run-1")}

	report := analyze(control)
	if !report.Ready() {
		t.Fatalf("expected clean snapshot to be recoverable, got block: %+v", report.Stages)
	}
	if stage, blocked := report.BlockedAt(); blocked {
		t.Fatalf("expected no blocked stage, got %s", stage)
	}
	if len(report.Runs) != 1 || report.Runs[0].ID != "run-1" {
		t.Fatalf("expected one recoverable run, got %+v", report.Runs)
	}
	if len(report.Routes) != 1 || report.Routes[0].RunID != "run-1" {
		t.Fatalf("expected one recoverable route, got %+v", report.Routes)
	}
}

func TestAnalyzeEvidenceJournalGapBlocksRuns(t *testing.T) {
	control := baseControl()
	control.Runs = []model.Run{testRun("run-1", model.StateLaunched)}

	report := analyzeRecoveryEvidence(journal.FileSnapshot{Version: 1, LastSequence: 9, Control: control}, nil)
	if report.Ready() {
		t.Fatal("expected snapshot ahead of journal to block recovery")
	}
	journalFinding := stageFinding(report, RecoveryJournal)
	if journalFinding.Recoverable || len(journalFinding.BlockingReasons) == 0 {
		t.Fatalf("expected journal stage blocking reasons, got %+v", journalFinding)
	}
	runsFinding := stageFinding(report, RecoveryRuns)
	if runsFinding.Recoverable {
		t.Fatal("expected runs stage to inherit the journal block")
	}
}

func TestAnalyzeEvidenceInvalidRunIsBlockerNotSilentDrop(t *testing.T) {
	control := baseControl()
	control.Runs = []model.Run{
		testRun("run-1", model.StateLaunched),
		{ID: "bad-run", State: model.StateLoaded}, // missing station and epoch
	}

	report := analyze(control)
	if report.Ready() {
		t.Fatal("expected invalid run to block recovery")
	}
	runsFinding := stageFinding(report, RecoveryRuns)
	if runsFinding.Recoverable || runsFinding.RecoverableCount != 1 {
		t.Fatalf("expected one counted run plus a blocker, got %+v", runsFinding)
	}
	if len(report.Runs) != 1 || report.Runs[0].ID != "run-1" {
		t.Fatalf("invalid run must not appear as recoverable: %+v", report.Runs)
	}
}

func TestAnalyzeEvidenceRouteReferencesMissingRun(t *testing.T) {
	control := baseControl()
	control.Runs = []model.Run{testRun("run-1", model.StateLaunched)}
	control.Routes = []model.RouteState{testReservedRoute("run-2")}

	report := analyze(control)
	routesFinding := stageFinding(report, RecoveryRoutes)
	if routesFinding.Recoverable || len(routesFinding.BlockingReasons) == 0 {
		t.Fatalf("expected route to missing run to block, got %+v", routesFinding)
	}
	if len(report.Routes) != 0 {
		t.Fatalf("dangling route must not be recoverable: %+v", report.Routes)
	}
	if stage, blocked := report.BlockedAt(); !blocked || stage != RecoveryRoutes {
		t.Fatalf("expected first block at routes, got %+v", report.Stages)
	}
}

func TestAnalyzeEvidenceRouteReferencesTerminalRun(t *testing.T) {
	control := baseControl()
	control.Runs = []model.Run{testRun("run-done", model.StateComplete)}
	control.Routes = []model.RouteState{testReservedRoute("run-done")}

	report := analyze(control)
	if stageFinding(report, RecoveryRoutes).Recoverable {
		t.Fatal("expected route fenced by terminal run to block")
	}
}

func TestAnalyzeEvidencePermitWithoutDurablePassage(t *testing.T) {
	control := baseControl()
	control.Runs = []model.Run{testRun("run-1", model.StateTracking)}
	control.Routes = []model.RouteState{testReservedRoute("run-1")}
	control.Permits = []model.PermitState{{
		RunID: "run-1", Name: "repressurization", Generation: 4, Granted: true,
	}}

	report := analyze(control)
	permitsFinding := stageFinding(report, RecoveryPermits)
	if permitsFinding.Recoverable || len(permitsFinding.BlockingReasons) == 0 {
		t.Fatalf("expected permit without durable passage to block, got %+v", permitsFinding)
	}
}

func TestAnalyzeEvidencePermitBackedByDurablePassageRecovers(t *testing.T) {
	control := baseControl()
	control.Runs = []model.Run{testRun("run-1", model.StateTracking)}
	control.Routes = []model.RouteState{testReservedRoute("run-1")}
	control.Permits = []model.PermitState{{
		RunID: "run-1", Name: "repressurization", Generation: 4, Granted: true, Reason: "ok",
	}}
	events := []model.Event{{RunID: "run-1", Kind: "passage", Epoch: 4, Durable: true}}

	report := analyze(control, events...)
	if !report.Ready() {
		t.Fatalf("expected permit backed by durable passage to recover, got %+v", report.Stages)
	}
	if len(report.Permits) != 1 || report.Permits[0].Generation != 4 {
		t.Fatalf("expected one recoverable permit, got %+v", report.Permits)
	}
}

func TestAnalyzeEvidenceResidualPressureDuringDepressurization(t *testing.T) {
	control := baseControl()
	control.Runs = []model.Run{testRun("run-1", model.StateDepressurizing)}
	control.Routes = []model.RouteState{testReservedRoute("run-1")}
	control.Permits = []model.PermitState{{
		RunID: "run-1", Name: "repressurization", Generation: 1, Granted: true,
	}}
	control.Pressures = []model.PressureState{{
		StationID: "station-a", RunID: "run-1", Value: 3.5, Target: 0,
		Mode: model.CompressorHolding, ObservedAt: time.Now().UTC(),
	}}
	events := []model.Event{{RunID: "run-1", Kind: "passage", Epoch: 1, Durable: true}}

	report := analyze(control, events...)
	pressureFinding := stageFinding(report, RecoveryPressure)
	if pressureFinding.Recoverable || len(pressureFinding.BlockingReasons) == 0 {
		t.Fatalf("expected residual depressurization pressure to block, got %+v", pressureFinding)
	}
}
