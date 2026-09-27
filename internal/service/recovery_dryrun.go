package service

import (
	"fmt"
	"sort"
	"time"

	"github.com/wyw14/solo-036/internal/journal"
	"github.com/wyw14/solo-036/internal/model"
)

// RecoveryEvidence is the batch of durable evidence a persistent recovery
// consumes: the pinned snapshot plus the journal it references. The dry run
// and the formal recovery both load evidence through readRecoveryEvidence so
// they always evaluate the same inputs. LoadError captures a journal-stage
// failure instead of aborting, so the dry run can report where a restart
// would get stuck.
type RecoveryEvidence struct {
	Snapshot      journal.FileSnapshot
	JournalEvents int
	LoadError     error
}

func readRecoveryEvidence(store *journal.FileStore) RecoveryEvidence {
	snapshot, err := store.ReadSnapshot()
	if err != nil {
		return RecoveryEvidence{LoadError: err}
	}
	events, err := store.Events()
	if err != nil {
		return RecoveryEvidence{LoadError: err}
	}
	return RecoveryEvidence{Snapshot: snapshot, JournalEvents: len(events)}
}

type SnapshotSummary struct {
	Version      int       `json:"version"`
	WrittenAt    time.Time `json:"written_at"`
	LastSequence int64     `json:"last_sequence"`
	LastChecksum string    `json:"last_checksum"`
}

type RecoveryStageStatus struct {
	Stage   RecoveryStage `json:"stage"`
	Ready   bool          `json:"ready"`
	Blocked string        `json:"blocked,omitempty"`
}

type RunRecovery struct {
	RunID       string         `json:"run_id"`
	Station     string         `json:"station"`
	State       model.RunState `json:"state"`
	Recoverable bool           `json:"recoverable"`
	Blocked     string         `json:"blocked,omitempty"`
}

type RouteRecovery struct {
	RouteID     string `json:"route_id"`
	RunID       string `json:"run_id"`
	LeaseEpoch  int64  `json:"lease_epoch"`
	Recoverable bool   `json:"recoverable"`
	Blocked     string `json:"blocked,omitempty"`
}

type PermitRecovery struct {
	RunID       string `json:"run_id"`
	Name        string `json:"name"`
	Generation  int64  `json:"generation"`
	Recoverable bool   `json:"recoverable"`
	Blocked     string `json:"blocked,omitempty"`
}

// RecoveryDryRun is the read-only preview of a persistent recovery: which
// runs, routes and permits a snapshot would restore, and the first recovery
// stage that would block a restart along with the reason.
type RecoveryDryRun struct {
	Persisted     bool                  `json:"persisted"`
	Ready         bool                  `json:"ready"`
	BlockedAt     RecoveryStage         `json:"blocked_at,omitempty"`
	JournalEvents int                   `json:"journal_events"`
	Snapshot      *SnapshotSummary      `json:"snapshot,omitempty"`
	Stages        []RecoveryStageStatus `json:"stages"`
	Runs          []RunRecovery         `json:"runs"`
	Routes        []RouteRecovery       `json:"routes"`
	Permits       []PermitRecovery      `json:"permits"`
}

// Blocker returns the reason the first unready stage is blocked, or "" when
// the dry run is fully ready.
func (r RecoveryDryRun) Blocker() string {
	for _, stage := range r.Stages {
		if !stage.Ready {
			return stage.Blocked
		}
	}
	return ""
}

// PlanRecovery evaluates a batch of recovery evidence without touching any
// live state. It walks the canonical recovery plan stage by stage, records
// which snapshot entries each stage would restore, and marks where the
// formal recovery would stop.
func PlanRecovery(evidence RecoveryEvidence) RecoveryDryRun {
	report := RecoveryDryRun{
		Persisted:     true,
		JournalEvents: evidence.JournalEvents,
		Stages:        make([]RecoveryStageStatus, 0, len(canonicalRecoveryOrder)),
		Runs:          make([]RunRecovery, 0),
		Routes:        make([]RouteRecovery, 0),
		Permits:       make([]PermitRecovery, 0),
	}
	if evidence.LoadError == nil {
		snapshot := evidence.Snapshot
		report.Snapshot = &SnapshotSummary{
			Version:      snapshot.Version,
			WrittenAt:    snapshot.WrittenAt,
			LastSequence: snapshot.LastSequence,
			LastChecksum: snapshot.LastChecksum,
		}
	}
	recoveredRuns := make(map[string]model.Run)
	if evidence.LoadError == nil {
		for _, record := range evidence.Snapshot.Control.Runs {
			item := RunRecovery{RunID: record.ID, Station: record.Station, State: record.State, Recoverable: true}
			if !model.ValidateRun(record) {
				item.Recoverable = false
				item.Blocked = "run record is incomplete: id, station, state and a positive epoch are required"
			} else {
				recoveredRuns[record.ID] = record
			}
			report.Runs = append(report.Runs, item)
		}
		for _, routeState := range evidence.Snapshot.Control.Routes {
			if !routeState.Reserved {
				continue
			}
			item := RouteRecovery{RouteID: routeState.ID, RunID: routeState.RunID, LeaseEpoch: routeState.LeaseEpoch, Recoverable: true}
			if routeState.RunID == "" || routeState.LeaseEpoch <= 0 {
				item.Recoverable = false
				item.Blocked = "reserved route has no fenced owner"
			} else if owner, ok := recoveredRuns[routeState.RunID]; !ok {
				item.Recoverable = false
				item.Blocked = fmt.Sprintf("route lease references missing run %s", routeState.RunID)
			} else if owner.State.Terminal() {
				item.Recoverable = false
				item.Blocked = fmt.Sprintf("route lease references terminal run %s (%s)", routeState.RunID, owner.State)
			}
			report.Routes = append(report.Routes, item)
		}
		for _, permitState := range evidence.Snapshot.Control.Permits {
			if !permitState.Granted {
				continue
			}
			item := PermitRecovery{RunID: permitState.RunID, Name: permitState.Name, Generation: permitState.Generation, Recoverable: true}
			if err := permitState.Validate(); err != nil {
				item.Recoverable = false
				item.Blocked = err.Error()
			} else if _, ok := recoveredRuns[permitState.RunID]; !ok {
				item.Recoverable = false
				item.Blocked = fmt.Sprintf("permit references missing run %s", permitState.RunID)
			}
			report.Permits = append(report.Permits, item)
		}
		sort.Slice(report.Runs, func(i, j int) bool { return report.Runs[i].RunID < report.Runs[j].RunID })
		sort.Slice(report.Routes, func(i, j int) bool { return report.Routes[i].RouteID < report.Routes[j].RouteID })
		sort.Slice(report.Permits, func(i, j int) bool {
			if report.Permits[i].RunID == report.Permits[j].RunID {
				return report.Permits[i].Name < report.Permits[j].Name
			}
			return report.Permits[i].RunID < report.Permits[j].RunID
		})
	}
	blockers := map[RecoveryStage]string{
		RecoveryJournal:  errorText(evidence.LoadError),
		RecoveryRuns:     firstBlocked(runBlockers(report.Runs)),
		RecoveryRoutes:   firstBlocked(routeBlockers(report.Routes)),
		RecoveryPermits:  firstBlocked(permitBlockers(report.Permits)),
		RecoveryPressure: "",
	}
	plan := NewRecoveryPlan()
	for _, stage := range canonicalRecoveryOrder {
		status := RecoveryStageStatus{Stage: stage, Ready: true}
		switch {
		case !plan.Ready(stage):
			status.Ready = false
			status.Blocked = fmt.Sprintf("stage %s has not completed", firstUnreadyDependency(plan, stage))
		case blockers[stage] != "":
			status.Ready = false
			status.Blocked = blockers[stage]
		}
		if status.Ready {
			if err := plan.Complete(stage); err != nil {
				status.Ready = false
				status.Blocked = err.Error()
			}
		}
		if !status.Ready && report.BlockedAt == "" {
			report.BlockedAt = stage
		}
		report.Stages = append(report.Stages, status)
	}
	report.Ready = report.BlockedAt == ""
	return report
}

// DryRunRecovery previews what a formal recovery from the persisted snapshot
// would do. It only reads the journal and snapshot; no runtime state is
// modified, so it is safe to call at any time before a restart.
func (r *Runtime) DryRunRecovery() RecoveryDryRun {
	control := r.ensureControl()
	if control.Persistent == nil {
		stages := make([]RecoveryStageStatus, 0, len(canonicalRecoveryOrder))
		for _, stage := range canonicalRecoveryOrder {
			stages = append(stages, RecoveryStageStatus{Stage: stage, Ready: true})
		}
		return RecoveryDryRun{
			Ready:   true,
			Stages:  stages,
			Runs:    make([]RunRecovery, 0),
			Routes:  make([]RouteRecovery, 0),
			Permits: make([]PermitRecovery, 0),
		}
	}
	return PlanRecovery(readRecoveryEvidence(control.Persistent))
}

func firstUnreadyDependency(plan *RecoveryPlan, stage RecoveryStage) RecoveryStage {
	for _, dependency := range plan.steps[stage].Requires {
		if !plan.steps[dependency].Completed {
			return dependency
		}
	}
	return ""
}

func firstBlocked(blockers []string) string {
	switch {
	case len(blockers) == 0:
		return ""
	case len(blockers) == 1:
		return blockers[0]
	default:
		return fmt.Sprintf("%s (and %d more)", blockers[0], len(blockers)-1)
	}
}

func runBlockers(items []RunRecovery) []string {
	blockers := make([]string, 0)
	for _, item := range items {
		if !item.Recoverable {
			blockers = append(blockers, fmt.Sprintf("run %s: %s", item.RunID, item.Blocked))
		}
	}
	return blockers
}

func routeBlockers(items []RouteRecovery) []string {
	blockers := make([]string, 0)
	for _, item := range items {
		if !item.Recoverable {
			blockers = append(blockers, fmt.Sprintf("route %s: %s", item.RouteID, item.Blocked))
		}
	}
	return blockers
}

func permitBlockers(items []PermitRecovery) []string {
	blockers := make([]string, 0)
	for _, item := range items {
		if !item.Recoverable {
			blockers = append(blockers, fmt.Sprintf("permit %s/%s: %s", item.RunID, item.Name, item.Blocked))
		}
	}
	return blockers
}
