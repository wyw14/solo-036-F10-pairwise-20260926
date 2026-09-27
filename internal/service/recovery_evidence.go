package service

import (
	"fmt"
	"sort"

	"github.com/wyw14/solo-036/internal/journal"
	"github.com/wyw14/solo-036/internal/model"
)

// RecoverableRun describes a run record that passed recovery evidence checks
// and can be republished into the run store.
type RecoverableRun struct {
	ID      string         `json:"id"`
	Station string         `json:"station"`
	State   model.RunState `json:"state"`
	Epoch   int64          `json:"epoch"`
}

// RecoverableRoute describes a reserved route lease that is fenced by a
// recoverable, non-terminal run.
type RecoverableRoute struct {
	ID    string `json:"id"`
	RunID string `json:"run_id"`
	Epoch int64  `json:"lease_epoch"`
}

// RecoverablePermit describes a granted permit whose durable passage evidence
// is present in the journal and whose run is recoverable.
type RecoverablePermit struct {
	RunID      string `json:"run_id"`
	Name       string `json:"name"`
	Generation int64  `json:"generation"`
}

// RecoveryStageFinding is the per-stage verdict of a recovery evidence
// analysis: what the stage would recover and why, if anything, blocks it.
type RecoveryStageFinding struct {
	Stage            RecoveryStage `json:"stage"`
	Recoverable      bool          `json:"recoverable"`
	RecoverableCount int           `json:"recoverable_count"`
	BlockingReasons  []string      `json:"blocking_reasons"`
}

// RecoveryEvidenceReport is the single verdict shape shared by the read-only
// dry run and the real recovery. The real recovery refuses to publish anything
// unless Ready() is true.
type RecoveryEvidenceReport struct {
	SnapshotSequence int64                  `json:"snapshot_sequence"`
	JournalEvents    int                    `json:"journal_events"`
	Runs             []RecoverableRun       `json:"runs"`
	Routes           []RecoverableRoute     `json:"routes"`
	Permits          []RecoverablePermit    `json:"permits"`
	Stages           []RecoveryStageFinding `json:"stages"`
}

// Ready reports whether every recovery stage passed its evidence checks.
func (r *RecoveryEvidenceReport) Ready() bool {
	for _, stage := range r.Stages {
		if !stage.Recoverable {
			return false
		}
	}
	return true
}

// BlockedAt returns the first stage in canonical recovery order that has
// blocking reasons, i.e. the step this snapshot would get stuck at.
func (r *RecoveryEvidenceReport) BlockedAt() (RecoveryStage, bool) {
	for _, stage := range r.Stages {
		if !stage.Recoverable {
			return stage.Stage, true
		}
	}
	return "", false
}

// analyzeRecoveryEvidence assembles and cross-checks the evidence of one
// snapshot against the journal events that must back it. It is a pure
// function: the dry run calls it on freshly read evidence and real recovery
// calls it again on evidence re-read immediately before publishing, so both
// paths use exactly the same recoverability rules.
func analyzeRecoveryEvidence(snapshot journal.FileSnapshot, events []model.Event) RecoveryEvidenceReport {
	control := snapshot.Control
	report := RecoveryEvidenceReport{
		SnapshotSequence: snapshot.LastSequence,
		JournalEvents:    len(events),
		Runs:             make([]RecoverableRun, 0),
		Routes:           make([]RecoverableRoute, 0),
		Permits:          make([]RecoverablePermit, 0),
		Stages:           make([]RecoveryStageFinding, 0, 5),
	}

	// Stage 1: journal — the durable log must at least cover the snapshot tail.
	journalReasons := make([]string, 0)
	if snapshot.LastSequence > int64(len(events)) {
		journalReasons = append(journalReasons, fmt.Sprintf(
			"snapshot references event sequence %d but journal contains %d events",
			snapshot.LastSequence, len(events)))
	}
	journalBlocked := len(journalReasons) > 0
	report.Stages = append(report.Stages, RecoveryStageFinding{
		Stage:           RecoveryJournal,
		Recoverable:     !journalBlocked,
		BlockingReasons: journalReasons,
	})

	// Stage 2: runs — every record must be structurally valid. Invalid records
	// are blockers rather than silently dropped, because dropping them would
	// publish a state that disagrees with the snapshot.
	runByID := make(map[string]model.Run, len(control.Runs))
	runReasons := make([]string, 0)
	for _, recorded := range control.Runs {
		if !model.ValidateRun(recorded) {
			runReasons = append(runReasons, fmt.Sprintf(
				"run %q is invalid (id, station, state and a positive epoch are required)",
				recorded.ID))
			continue
		}
		runByID[recorded.ID] = recorded
		report.Runs = append(report.Runs, RecoverableRun{
			ID:      recorded.ID,
			Station: recorded.Station,
			State:   recorded.State,
			Epoch:   recorded.Epoch,
		})
	}
	sort.Slice(report.Runs, func(i, j int) bool { return report.Runs[i].ID < report.Runs[j].ID })
	runsBlocked := journalBlocked || len(runReasons) > 0
	if journalBlocked {
		runReasons = appendDependencyBlock(runReasons, RecoveryJournal)
	}
	report.Stages = append(report.Stages, RecoveryStageFinding{
		Stage:            RecoveryRuns,
		Recoverable:      !runsBlocked,
		RecoverableCount: len(report.Runs),
		BlockingReasons:  runReasons,
	})

	// Stage 3: routes — every reserved lease must be owned by a recoverable,
	// non-terminal run (the same invariant RecoveryBarrier enforces live).
	routeReasons := make([]string, 0)
	for _, routeState := range control.Routes {
		if !routeState.Reserved {
			continue
		}
		if routeState.RunID == "" || routeState.LeaseEpoch <= 0 {
			routeReasons = append(routeReasons, fmt.Sprintf(
				"route %s is reserved without a fenced owner", routeState.ID))
			continue
		}
		owner, ok := runByID[routeState.RunID]
		if !ok {
			routeReasons = append(routeReasons, fmt.Sprintf(
				"route %s is reserved by missing run %s", routeState.ID, routeState.RunID))
			continue
		}
		if owner.State.Terminal() {
			routeReasons = append(routeReasons, fmt.Sprintf(
				"route %s is reserved by terminal run %s (state %s)",
				routeState.ID, owner.ID, owner.State))
			continue
		}
		report.Routes = append(report.Routes, RecoverableRoute{
			ID:    routeState.ID,
			RunID: routeState.RunID,
			Epoch: routeState.LeaseEpoch,
		})
	}
	sort.Slice(report.Routes, func(i, j int) bool { return report.Routes[i].ID < report.Routes[j].ID })
	routesBlocked := runsBlocked || len(routeReasons) > 0
	if runsBlocked {
		routeReasons = appendDependencyBlock(routeReasons, RecoveryRuns)
	}
	report.Stages = append(report.Stages, RecoveryStageFinding{
		Stage:            RecoveryRoutes,
		Recoverable:      !routesBlocked,
		RecoverableCount: len(report.Routes),
		BlockingReasons:  routeReasons,
	})

	// Stage 4: permits — each granted permit needs a durable passage event at
	// its generation in the journal and a recoverable owner.
	durablePassages := make(map[string][]model.Event)
	for _, event := range events {
		if event.Kind == "passage" && event.Durable {
			durablePassages[event.RunID] = append(durablePassages[event.RunID], event)
		}
	}
	permitReasons := make([]string, 0)
	for _, permitState := range control.Permits {
		if !permitState.Granted {
			continue
		}
		owner, ok := runByID[permitState.RunID]
		if !ok {
			permitReasons = append(permitReasons, fmt.Sprintf(
				"permit %s is granted to missing run %s", permitState.Name, permitState.RunID))
			continue
		}
		if owner.State.Terminal() {
			permitReasons = append(permitReasons, fmt.Sprintf(
				"permit %s is granted to terminal run %s", permitState.Name, owner.ID))
			continue
		}
		if !hasPassageAtGeneration(durablePassages[permitState.RunID], permitState.Generation) {
			permitReasons = append(permitReasons, fmt.Sprintf(
				"permit %s for run %s at generation %d has no durable passage event in the journal",
				permitState.Name, permitState.RunID, permitState.Generation))
			continue
		}
		report.Permits = append(report.Permits, RecoverablePermit{
			RunID:      permitState.RunID,
			Name:       permitState.Name,
			Generation: permitState.Generation,
		})
	}
	sort.Slice(report.Permits, func(i, j int) bool {
		if report.Permits[i].RunID != report.Permits[j].RunID {
			return report.Permits[i].RunID < report.Permits[j].RunID
		}
		return report.Permits[i].Name < report.Permits[j].Name
	})
	permitsBlocked := routesBlocked || len(permitReasons) > 0
	if routesBlocked {
		permitReasons = appendDependencyBlock(permitReasons, RecoveryRoutes)
	}
	report.Stages = append(report.Stages, RecoveryStageFinding{
		Stage:            RecoveryPermits,
		Recoverable:      !permitsBlocked,
		RecoverableCount: len(report.Permits),
		BlockingReasons:  permitReasons,
	})

	// Stage 5: pressure — active pressure must belong to a recoverable run and
	// be fenced by a recovered route lease; pressure past the launcher also
	// needs a recovered permit, and residual pressure during depressurization is
	// a critical emergency condition.
	routesByRun := make(map[string][]RecoverableRoute)
	for _, leased := range report.Routes {
		routesByRun[leased.RunID] = append(routesByRun[leased.RunID], leased)
	}
	permitsByRun := make(map[string][]RecoverablePermit)
	for _, granted := range report.Permits {
		permitsByRun[granted.RunID] = append(permitsByRun[granted.RunID], granted)
	}
	pressureReasons := make([]string, 0)
	activePressure := 0
	for _, pressureState := range control.Pressures {
		if pressureState.RunID == "" || pressureState.Value == 0 {
			continue
		}
		activePressure++
		owner, ok := runByID[pressureState.RunID]
		if !ok {
			pressureReasons = append(pressureReasons, fmt.Sprintf(
				"pressure %.2f at station %s references missing run %s",
				pressureState.Value, pressureState.StationID, pressureState.RunID))
			continue
		}
		if owner.State == model.StateDepressurizing {
			pressureReasons = append(pressureReasons, fmt.Sprintf(
				"run %s is depressurizing but pressure %.2f remains at station %s",
				owner.ID, pressureState.Value, pressureState.StationID))
			continue
		}
		if len(routesByRun[owner.ID]) == 0 {
			pressureReasons = append(pressureReasons, fmt.Sprintf(
				"pressure %.2f at station %s for run %s is not fenced by a recovered route lease",
				pressureState.Value, pressureState.StationID, owner.ID))
		}
		if requiresPermit(owner.State) && len(permitsByRun[owner.ID]) == 0 {
			pressureReasons = append(pressureReasons, fmt.Sprintf(
				"pressure %.2f at station %s for run %s in state %s is not fenced by a recovered permit",
				pressureState.Value, pressureState.StationID, owner.ID, owner.State))
		}
	}
	pressureBlocked := permitsBlocked || len(pressureReasons) > 0
	if permitsBlocked {
		pressureReasons = appendDependencyBlock(pressureReasons, RecoveryPermits)
	}
	report.Stages = append(report.Stages, RecoveryStageFinding{
		Stage:            RecoveryPressure,
		Recoverable:      !pressureBlocked,
		RecoverableCount: activePressure,
		BlockingReasons:  pressureReasons,
	})

	return report
}

func hasPassageAtGeneration(events []model.Event, generation int64) bool {
	for _, event := range events {
		if event.Epoch == generation {
			return true
		}
	}
	return false
}

// requiresPermit reports whether a run state is downstream of the first
// intermediate-station passage, where repressurization must be backed by a
// durable passage permit.
func requiresPermit(state model.RunState) bool {
	switch state {
	case model.StateTracking, model.StateReceiving, model.StateDepressurizing:
		return true
	default:
		return false
	}
}

func appendDependencyBlock(reasons []string, dependency RecoveryStage) []string {
	return append(reasons, fmt.Sprintf("blocked because recovery stage %s did not pass", dependency))
}
