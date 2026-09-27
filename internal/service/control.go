package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/wyw14/solo-036/internal/emergency"
	"github.com/wyw14/solo-036/internal/journal"
	"github.com/wyw14/solo-036/internal/launcher"
	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/permit"
	"github.com/wyw14/solo-036/internal/receiver"
	"github.com/wyw14/solo-036/internal/route"
	"github.com/wyw14/solo-036/internal/run"
	"github.com/wyw14/solo-036/internal/tracker"
)

type ControlState struct {
	Network    *route.Network
	Stations   map[string]model.Station
	Routes     map[string]model.RouteState
	Valves     map[string]model.ValveState
	Closures   map[string]model.ClosureState
	Pressures  map[string]model.PressureState
	Tracking   map[string][]model.TrackingState
	Permits    map[string][]model.PermitState
	Incidents  []model.Incident
	Revision   string
	Persistent *journal.FileStore
}

type LaunchRequest struct {
	StationID      string  `json:"station_id"`
	RouteID        string  `json:"route_id"`
	TargetPressure float64 `json:"target_pressure"`
}

type TrackingRequest struct {
	StationID  string             `json:"station_id"`
	SourceID   string             `json:"source_id"`
	Kind       tracker.SourceKind `json:"kind"`
	Position   float64            `json:"position"`
	ObservedAt time.Time          `json:"observed_at"`
}

type ReceiveRequest struct {
	StationID      string `json:"station_id"`
	IsolationEpoch int64  `json:"isolation_epoch"`
}

type RecoveryReport struct {
	RecoveredRuns    int                    `json:"recovered_runs"`
	RecoveredRoutes  int                    `json:"recovered_routes"`
	RecoveredPermits int                    `json:"recovered_permits"`
	JournalEvents    int                    `json:"journal_events"`
	Ready            bool                   `json:"ready"`
	Stages           []RecoveryStageFinding `json:"stages,omitempty"`
}

func defaultControlState() *ControlState {
	network := route.DefaultNetwork()
	stations := network.Stations()
	state := &ControlState{
		Network:   network,
		Stations:  make(map[string]model.Station),
		Routes:    make(map[string]model.RouteState),
		Valves:    make(map[string]model.ValveState),
		Closures:  make(map[string]model.ClosureState),
		Pressures: make(map[string]model.PressureState),
		Tracking:  make(map[string][]model.TrackingState),
		Permits:   make(map[string][]model.PermitState),
		Incidents: make([]model.Incident, 0),
		Revision:  model.NewRevision(),
	}
	for _, station := range stations {
		state.Stations[station.ID] = station
		state.Pressures[station.ID] = model.PressureState{
			StationID:  station.ID,
			Value:      0,
			Target:     0,
			Mode:       model.CompressorStandby,
			ObservedAt: time.Now().UTC(),
		}
	}
	for _, routeState := range network.RouteStates() {
		state.Routes[routeState.ID] = routeState
	}
	for _, stationID := range []string{"station-a", "station-b", "station-c"} {
		state.Valves[stationID+"/main"] = model.ValveState{
			ID:         stationID + "/main",
			StationID:  stationID,
			Position:   model.ValveClosed,
			ObservedAt: time.Now().UTC(),
		}
	}
	return state
}

func (r *Runtime) ensureControl() *ControlState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.control == nil {
		r.control = defaultControlState()
	}
	return r.control
}

func (r *Runtime) ConfigurePersistence(directory string) error {
	store, err := journal.NewFileStore(directory)
	if err != nil {
		return err
	}
	if err := store.Verify(); err != nil {
		return fmt.Errorf("verify persistence: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.control == nil {
		r.control = defaultControlState()
	}
	r.control.Persistent = store
	return nil
}

func (r *Runtime) appendEvent(event model.Event) error {
	if event.ID == "" {
		event.ID = model.NewRevision()
	}
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	if err := r.Journal.Append(event); err != nil {
		return err
	}
	control := r.ensureControl()
	if control.Persistent != nil {
		if err := control.Persistent.Append(event); err != nil {
			return fmt.Errorf("persist event: %w", err)
		}
	}
	return nil
}

func (r *Runtime) recordIncident(runID, severity, message string) model.Incident {
	incident := model.Incident{
		ID:       model.NewRevision(),
		RunID:    runID,
		Severity: severity,
		Message:  message,
		At:       time.Now().UTC(),
	}
	r.mu.Lock()
	if r.control == nil {
		r.control = defaultControlState()
	}
	r.control.Incidents = append(r.control.Incidents, incident)
	r.control.Revision = model.NewRevision()
	r.mu.Unlock()
	_ = r.appendEvent(model.Event{
		RunID:   runID,
		Kind:    "incident",
		Payload: severity + ":" + message,
	})
	return incident
}

func (r *Runtime) ValidateLaunchRequest(request LaunchRequest) error {
	control := r.ensureControl()
	station, ok := control.Stations[request.StationID]
	if !ok {
		return fmt.Errorf("unknown launch station %s", request.StationID)
	}
	if station.Role != model.StationLauncher || !station.Enabled {
		return errors.New("station is not an enabled launcher")
	}
	routeState, ok := control.Routes[request.RouteID]
	if !ok {
		return fmt.Errorf("unknown route %s", request.RouteID)
	}
	if routeState.FromStation != station.ID {
		return errors.New("route does not start at launcher")
	}
	if _, err := control.Network.Path(routeState.FromStation, routeState.ToStation, request.TargetPressure); err != nil {
		return err
	}
	if request.TargetPressure <= 0 || request.TargetPressure > station.PressureLimit {
		return fmt.Errorf("target pressure must be within 0 and %.2f", station.PressureLimit)
	}
	return nil
}

func (r *Runtime) Launch(ctx context.Context, request LaunchRequest) (model.OperationRecord, error) {
	if err := r.ValidateLaunchRequest(request); err != nil {
		return model.OperationRecord{}, err
	}
	created, err := r.CreateOperation(request.StationID)
	if err != nil {
		return model.OperationRecord{}, err
	}
	lease, routeErr := r.Routes.Reserve(request.RouteID, created.ID, created.Epoch)
	if routeErr != nil {
		r.recordIncident(created.ID, "warning", routeErr.Error())
		return model.OperationRecord{}, routeErr
	}
	closureEpoch := created.Epoch + 1
	r.Launcher.BeginClosure(created.ID, closureEpoch)
	closureProof := model.CommandReceipt{
		RunID:    created.ID,
		Command:  "closure",
		Epoch:    closureEpoch,
		Accepted: true,
		At:       time.Now().UTC(),
	}
	valveErr := r.Launcher.ConfirmClosure(closureProof)
	if err := launcher.CombineLaunchResults(valveErr, routeErr); err != nil {
		r.Routes.Release(request.RouteID, created.ID)
		return model.OperationRecord{}, err
	}
	if !r.Launcher.CanLaunch(closureProof) {
		r.Routes.Release(request.RouteID, created.ID)
		return model.OperationRecord{}, errors.New("launcher interlocks rejected current closure proof")
	}
	if err := r.Pressure.Apply(created.ID, request.TargetPressure); err != nil {
		r.Routes.Release(request.RouteID, created.ID)
		return model.OperationRecord{}, err
	}
	if err := run.Start(r.Runs, created.ID); err != nil {
		r.Pressure.Stop()
		r.Routes.Release(request.RouteID, created.ID)
		return model.OperationRecord{}, err
	}
	now := time.Now().UTC()
	r.mu.Lock()
	control := r.control
	routeState := control.Routes[request.RouteID]
	routeState.RunID = created.ID
	routeState.LeaseEpoch = lease.Epoch
	routeState.Reserved = true
	routeState.UpdatedAt = now
	control.Routes[request.RouteID] = routeState
	control.Closures[request.StationID] = model.ClosureState{
		StationID:    request.StationID,
		RunID:        created.ID,
		CommandEpoch: closureEpoch,
		Locked:       true,
		ObservedAt:   now,
	}
	pressureState := control.Pressures[request.StationID]
	pressureState.RunID = created.ID
	pressureState.Value = request.TargetPressure
	pressureState.Target = request.TargetPressure
	pressureState.Mode = model.CompressorHolding
	pressureState.ObservedAt = now
	control.Pressures[request.StationID] = pressureState
	valveState := control.Valves[request.StationID+"/main"]
	valveState.RunID = created.ID
	valveState.Position = model.ValveOpen
	valveState.CommandEpoch = closureEpoch
	valveState.ObservedAt = now
	control.Valves[valveState.ID] = valveState
	control.Revision = model.NewRevision()
	r.mu.Unlock()
	if err := r.appendEvent(model.Event{
		RunID:   created.ID,
		Kind:    "launch",
		Epoch:   closureEpoch,
		Payload: request.RouteID,
		At:      now,
	}); err != nil {
		return model.OperationRecord{}, err
	}
	select {
	case <-ctx.Done():
		_ = r.Cancel(created.ID)
		return model.OperationRecord{}, ctx.Err()
	default:
	}
	return r.Operation(created.ID)
}

func (r *Runtime) Track(runID string, request TrackingRequest) (model.OperationRecord, error) {
	if strings.TrimSpace(request.SourceID) == "" {
		return model.OperationRecord{}, errors.New("tracking source is required")
	}
	control := r.ensureControl()
	station, ok := control.Stations[request.StationID]
	if !ok || !station.Enabled {
		return model.OperationRecord{}, errors.New("tracking station is unavailable")
	}
	current, ok := r.Runs.Get(runID)
	if !ok {
		return model.OperationRecord{}, fmt.Errorf("run %s not found", runID)
	}
	if current.State == model.StateLaunched {
		if err := run.Track(r.Runs, runID); err != nil {
			return model.OperationRecord{}, err
		}
	}
	if request.ObservedAt.IsZero() {
		request.ObservedAt = time.Now().UTC()
	}
	if request.Kind == "" {
		request.Kind = tracker.SourceMagnetic
	}
	evidence := r.Tracker.Observe(runID, request.StationID, request.SourceID, request.Position, request.ObservedAt)
	if err := r.Lineage.Add(tracker.SourceObservation{
		RunID:            runID,
		StationID:        request.StationID,
		ObservationID:    model.NewRevision(),
		PhysicalSourceID: request.SourceID,
		Kind:             request.Kind,
		Position:         request.Position,
		Generation:       evidence.Generation,
		ObservedAt:       request.ObservedAt,
		Valid:            true,
	}); err != nil {
		return model.OperationRecord{}, err
	}
	if err := r.Routes.Advance(runID, request.Position); err != nil {
		r.recordIncident(runID, "warning", err.Error())
		return model.OperationRecord{}, err
	}
	state := model.TrackingState{
		RunID:      runID,
		StationID:  request.StationID,
		SourceID:   request.SourceID,
		Generation: evidence.Generation,
		Position:   request.Position,
		Online:     true,
		ObservedAt: request.ObservedAt,
	}
	r.mu.Lock()
	r.control.Tracking[runID] = append(r.control.Tracking[runID], state)
	r.control.Revision = model.NewRevision()
	r.mu.Unlock()
	if err := r.appendEvent(model.Event{
		RunID:   runID,
		Kind:    "tracking",
		Epoch:   evidence.Generation,
		Payload: fmt.Sprintf("%s/%s/%.3f", request.StationID, request.SourceID, request.Position),
		At:      request.ObservedAt,
	}); err != nil {
		return model.OperationRecord{}, err
	}
	return r.Operation(runID)
}

func (r *Runtime) GrantPassage(runID, stationID string) (model.OperationRecord, error) {
	control := r.ensureControl()
	if _, ok := control.Stations[stationID]; !ok {
		return model.OperationRecord{}, errors.New("unknown passage station")
	}
	generation := r.Tracker.Generation(runID)
	points := r.Lineage.Evidence(runID, stationID, generation)
	if len(points) < 2 {
		return model.OperationRecord{}, errors.New("insufficient passage evidence")
	}
	if !r.Tracker.Clearance(runID, stationID, 90*time.Second) {
		return model.OperationRecord{}, errors.New("passage evidence is not continuous")
	}
	if !r.Tracker.ArrivalQuorum(points, 2) || !tracker.IndependentPhysicalArrival(points, 2) {
		return model.OperationRecord{}, errors.New("passage evidence does not have independent physical sources")
	}
	proof := points[len(points)-1]
	if !permit.EvaluateRepressurization(r.Journal, r.Permits, proof) {
		return model.OperationRecord{}, errors.New("passage proof was not durable")
	}
	permitState := model.PermitState{
		RunID:      runID,
		Name:       "repressurization",
		Generation: proof.Generation,
		Granted:    true,
		Reason:     "continuous durable passage evidence",
		UpdatedAt:  time.Now().UTC(),
	}
	r.mu.Lock()
	r.control.Permits[runID] = append(r.control.Permits[runID], permitState)
	r.control.Revision = model.NewRevision()
	r.mu.Unlock()
	return r.Operation(runID)
}

func (r *Runtime) Receive(runID string, request ReceiveRequest) (model.OperationRecord, error) {
	control := r.ensureControl()
	station, ok := control.Stations[request.StationID]
	if !ok || station.Role != model.StationReceiver {
		return model.OperationRecord{}, errors.New("receiver station is invalid")
	}
	current, ok := r.Runs.Get(runID)
	if !ok {
		return model.OperationRecord{}, errors.New("run not found")
	}
	if current.State != model.StateTracking {
		return model.OperationRecord{}, fmt.Errorf("run state %s cannot enter receiver", current.State)
	}
	r.Valves.BeginIsolation(runID, request.IsolationEpoch)
	r.Valves.CompleteIsolation(runID, request.IsolationEpoch)
	receiver.Prepare(r.Receiver, runID, request.IsolationEpoch)
	proof := r.Valves.IsolationProof(runID, request.IsolationEpoch)
	if err := r.Receiver.Open(runID, proof); err != nil {
		return model.OperationRecord{}, err
	}
	if err := run.Receive(r.Runs, runID); err != nil {
		return model.OperationRecord{}, err
	}
	if err := emergency.Execute(r.Valves, r.Pressure, runID, request.IsolationEpoch); err != nil {
		return model.OperationRecord{}, err
	}
	r.mu.Lock()
	pressureState := r.control.Pressures[request.StationID]
	pressureState.RunID = runID
	pressureState.Value = 0
	pressureState.Target = 0
	pressureState.Mode = model.CompressorStopped
	pressureState.ObservedAt = time.Now().UTC()
	r.control.Pressures[request.StationID] = pressureState
	valveState := r.control.Valves[request.StationID+"/main"]
	valveState.RunID = runID
	valveState.CommandEpoch = request.IsolationEpoch
	valveState.Position = model.ValveClosed
	valveState.ObservedAt = time.Now().UTC()
	r.control.Valves[valveState.ID] = valveState
	r.control.Revision = model.NewRevision()
	r.mu.Unlock()
	if err := r.appendEvent(model.Event{
		RunID:   runID,
		Kind:    "receiving",
		Epoch:   request.IsolationEpoch,
		Payload: request.StationID,
	}); err != nil {
		return model.OperationRecord{}, err
	}
	return r.Operation(runID)
}

func (r *Runtime) Complete(runID, routeID string) (model.OperationRecord, error) {
	if err := run.Complete(r.Runs, runID); err != nil {
		return model.OperationRecord{}, err
	}
	r.Routes.Release(routeID, runID)
	r.mu.Lock()
	if routeState, ok := r.control.Routes[routeID]; ok && routeState.RunID == runID {
		routeState.RunID = ""
		routeState.Reserved = false
		routeState.LeaseEpoch = 0
		routeState.UpdatedAt = time.Now().UTC()
		r.control.Routes[routeID] = routeState
	}
	r.control.Revision = model.NewRevision()
	r.mu.Unlock()
	if err := r.appendEvent(model.Event{
		RunID:   runID,
		Kind:    "complete",
		Payload: routeID,
	}); err != nil {
		return model.OperationRecord{}, err
	}
	return r.Operation(runID)
}

func (r *Runtime) Cancel(runID string) error {
	if err := r.Runs.Cancel(runID); err != nil {
		return err
	}
	r.Pressure.Stop()
	r.mu.Lock()
	for id, routeState := range r.control.Routes {
		if routeState.RunID == runID {
			r.Routes.Release(id, runID)
			routeState.RunID = ""
			routeState.Reserved = false
			routeState.LeaseEpoch = 0
			routeState.UpdatedAt = time.Now().UTC()
			r.control.Routes[id] = routeState
		}
	}
	r.control.Revision = model.NewRevision()
	r.mu.Unlock()
	return r.appendEvent(model.Event{
		RunID:   runID,
		Kind:    "cancelled",
		Payload: "operator cancellation",
	})
}

func (r *Runtime) Operation(runID string) (model.OperationRecord, error) {
	snapshot := r.Snapshot()
	operation, ok := snapshot.Operation(runID)
	if !ok {
		return model.OperationRecord{}, fmt.Errorf("run %s not found", runID)
	}
	return operation, nil
}

func (r *Runtime) Snapshot() model.ControlSnapshot {
	control := r.ensureControl()
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := model.ControlSnapshot{
		Revision:   control.Revision,
		CapturedAt: time.Now().UTC(),
		Stations:   make([]model.Station, 0, len(control.Stations)),
		Valves:     make([]model.ValveState, 0, len(control.Valves)),
		Closures:   make([]model.ClosureState, 0, len(control.Closures)),
		Pressures:  make([]model.PressureState, 0, len(control.Pressures)),
		Routes:     make([]model.RouteState, 0, len(control.Routes)),
		Tracking:   make([]model.TrackingState, 0),
		Permits:    make([]model.PermitState, 0),
		Runs:       r.Runs.Snapshot(),
		Incidents:  append([]model.Incident(nil), control.Incidents...),
	}
	for _, value := range control.Stations {
		snapshot.Stations = append(snapshot.Stations, value)
	}
	snapshot.Stations = snapshot.SortedStations()
	for _, value := range control.Valves {
		snapshot.Valves = append(snapshot.Valves, value)
	}
	for _, value := range control.Closures {
		snapshot.Closures = append(snapshot.Closures, value)
	}
	for _, value := range control.Pressures {
		snapshot.Pressures = append(snapshot.Pressures, value)
	}
	for _, value := range control.Routes {
		snapshot.Routes = append(snapshot.Routes, value)
	}
	for _, values := range control.Tracking {
		snapshot.Tracking = append(snapshot.Tracking, values...)
	}
	for _, values := range control.Permits {
		snapshot.Permits = append(snapshot.Permits, values...)
	}
	sort.Slice(snapshot.Valves, func(i, j int) bool { return snapshot.Valves[i].ID < snapshot.Valves[j].ID })
	sort.Slice(snapshot.Routes, func(i, j int) bool { return snapshot.Routes[i].ID < snapshot.Routes[j].ID })
	sort.Slice(snapshot.Tracking, func(i, j int) bool { return snapshot.Tracking[i].ObservedAt.Before(snapshot.Tracking[j].ObservedAt) })
	return snapshot
}

func (r *Runtime) PersistSnapshot() error {
	control := r.ensureControl()
	if control.Persistent == nil {
		return nil
	}
	snapshot := r.Snapshot()
	if err := snapshot.Validate(); err != nil {
		return err
	}
	return control.Persistent.WriteSnapshot(snapshot)
}

// RecoverPersistent restores runtime state from the durable snapshot. The
// evidence (snapshot + full event journal) is re-read from disk on every call
// — conclusions from DryRunRecovery are never reused — and validated by the
// same analyzeRecoveryEvidence rules the dry run reports on. If any recovery
// stage is blocked, nothing is published. The publish itself is staged: all
// new state is assembled first, swapped in via infallible ReplaceAll
// operations, and verified by RecoveryBarrier afterwards; a failed post-check
// rolls the pre-recovery state back in so a half-recovered state is never
// visible.
func (r *Runtime) RecoverPersistent() (RecoveryReport, error) {
	control := r.ensureControl()
	r.mu.Lock()
	persistent := control.Persistent
	r.mu.Unlock()
	if persistent == nil {
		return RecoveryReport{Ready: true}, nil
	}
	snapshot, err := persistent.ReadSnapshot()
	if err != nil {
		return RecoveryReport{}, err
	}
	events, err := persistent.Events()
	if err != nil {
		return RecoveryReport{}, fmt.Errorf("read event journal: %w", err)
	}

	evidence := analyzeRecoveryEvidence(snapshot, events)
	if !evidence.Ready() {
		stage, _ := evidence.BlockedAt()
		return RecoveryReport{Stages: evidence.Stages}, fmt.Errorf(
			"recovery blocked at stage %s: snapshot evidence does not satisfy recovery requirements", stage)
	}

	// Drive the canonical stage order so a future stage addition cannot
	// silently bypass the journal → runs → routes → permits → pressure chain.
	plan := NewRecoveryPlan()
	if err := plan.Validate(); err != nil {
		return RecoveryReport{}, err
	}
	for _, stage := range []RecoveryStage{RecoveryJournal, RecoveryRuns, RecoveryRoutes, RecoveryPermits, RecoveryPressure} {
		if !plan.Ready(stage) {
			return RecoveryReport{}, fmt.Errorf("recovery stage %s is not ready in canonical order", stage)
		}
		if err := plan.Complete(stage); err != nil {
			return RecoveryReport{}, err
		}
	}

	// Assemble the full recovered state from the validated evidence. Ready()
	// guarantees every snapshot run and reserved lease passed its checks, so
	// filtering by the evidence lists preserves full records safely.
	runIDs := make(map[string]bool, len(evidence.Runs))
	for _, recovered := range evidence.Runs {
		runIDs[recovered.ID] = true
	}
	runs := make([]model.Run, 0, len(evidence.Runs))
	for _, recorded := range snapshot.Control.Runs {
		if runIDs[recorded.ID] {
			runs = append(runs, recorded)
		}
	}
	routeIDs := make(map[string]bool, len(evidence.Routes))
	for _, recovered := range evidence.Routes {
		routeIDs[recovered.ID] = true
	}
	leases := make([]model.RouteLease, 0, len(evidence.Routes))
	for _, routeState := range snapshot.Control.Routes {
		if routeState.Reserved && routeIDs[routeState.ID] {
			leases = append(leases, model.RouteLease{
				Route:      routeState.ID,
				RunID:      routeState.RunID,
				Epoch:      routeState.LeaseEpoch,
				AcquiredAt: routeState.UpdatedAt,
			})
		}
	}
	permitEvents := make([]model.Event, 0, len(evidence.Permits))
	for _, granted := range evidence.Permits {
		permitEvents = append(permitEvents, model.Event{
			RunID:   granted.RunID,
			Kind:    "passage",
			Epoch:   granted.Generation,
			Payload: "recovered durable passage",
			Durable: true,
		})
	}

	// Capture the pre-recovery state for rollback. All of these are copies.
	priorRuns := r.Runs.Snapshot()
	priorLeases := route.Snapshot(r.Routes)
	priorPermits := r.Permits.SnapshotEvents()
	r.mu.Lock()
	priorControl := r.control
	r.mu.Unlock()

	// Publish. Every operation below is infallible, so the swap cannot stop
	// halfway; the barrier check afterwards validates the combined result.
	r.Runs.ReplaceAll(runs)
	r.Routes.ReplaceAll(leases)
	r.Permits.ReplaceAll(permitEvents)
	r.mu.Lock()
	r.control = &ControlState{
		Network:    priorControl.Network,
		Stations:   snapshot.Control.StationIndex(),
		Routes:     make(map[string]model.RouteState),
		Valves:     make(map[string]model.ValveState),
		Closures:   make(map[string]model.ClosureState),
		Pressures:  make(map[string]model.PressureState),
		Tracking:   make(map[string][]model.TrackingState),
		Permits:    make(map[string][]model.PermitState),
		Incidents:  append([]model.Incident(nil), snapshot.Control.Incidents...),
		Revision:   model.NewRevision(),
		Persistent: persistent,
	}
	for _, value := range snapshot.Control.Routes {
		r.control.Routes[value.ID] = value
	}
	for _, value := range snapshot.Control.Valves {
		r.control.Valves[value.ID] = value
	}
	for _, value := range snapshot.Control.Closures {
		r.control.Closures[value.StationID] = value
	}
	for _, value := range snapshot.Control.Pressures {
		r.control.Pressures[value.StationID] = value
	}
	for _, value := range snapshot.Control.Tracking {
		r.control.Tracking[value.RunID] = append(r.control.Tracking[value.RunID], value)
	}
	for _, value := range snapshot.Control.Permits {
		r.control.Permits[value.RunID] = append(r.control.Permits[value.RunID], value)
	}
	r.mu.Unlock()

	if err := r.validateRecoveredState(); err != nil {
		// Roll the pre-recovery state back in; never leave a half-recovered
		// runtime visible to operators.
		r.Runs.ReplaceAll(priorRuns)
		r.Routes.ReplaceAll(priorLeases)
		r.Permits.ReplaceAll(priorPermits)
		r.mu.Lock()
		r.control = priorControl
		r.mu.Unlock()
		return RecoveryReport{Stages: evidence.Stages}, fmt.Errorf("recovery post-check failed, rolled back: %w", err)
	}

	return RecoveryReport{
		RecoveredRuns:    len(runs),
		RecoveredRoutes:  len(leases),
		RecoveredPermits: len(permitEvents),
		JournalEvents:    len(events),
		Ready:            true,
		Stages:           evidence.Stages,
	}, nil
}

func (r *Runtime) InterlockStatus() []model.Interlock {
	snapshot := r.Snapshot()
	interlocks := make([]model.Interlock, 0)
	for _, routeState := range snapshot.Routes {
		ready := !routeState.Reserved || routeState.RunID != ""
		reason := "route is available"
		if routeState.Reserved {
			reason = fmt.Sprintf("fenced to %s at epoch %d", routeState.RunID, routeState.LeaseEpoch)
		}
		interlocks = append(interlocks, model.Interlock{Name: "route/" + routeState.ID, Ready: ready, Reason: reason})
	}
	for _, pressureState := range snapshot.Pressures {
		station := snapshot.StationIndex()[pressureState.StationID]
		ready := pressureState.Value <= station.PressureLimit
		reason := fmt.Sprintf("pressure %.2f of limit %.2f", pressureState.Value, station.PressureLimit)
		interlocks = append(interlocks, model.Interlock{Name: "pressure/" + pressureState.StationID, Ready: ready, Reason: reason})
	}
	for _, closure := range snapshot.Closures {
		reason := "closure is not locked"
		if closure.Locked {
			reason = fmt.Sprintf("locked for %s at epoch %d", closure.RunID, closure.CommandEpoch)
		}
		interlocks = append(interlocks, model.Interlock{Name: "closure/" + closure.StationID, Ready: closure.Locked, Reason: reason})
	}
	sort.Slice(interlocks, func(i, j int) bool { return interlocks[i].Name < interlocks[j].Name })
	return interlocks
}

func (r *Runtime) EquipmentStatus() []model.Equipment {
	snapshot := r.Snapshot()
	equipment := make([]model.Equipment, 0, len(snapshot.Valves)+len(snapshot.Pressures)+len(snapshot.Closures))
	for _, valveState := range snapshot.Valves {
		equipment = append(equipment, model.Equipment{
			Name:      valveState.ID,
			Kind:      "valve",
			State:     string(valveState.Position),
			UpdatedAt: valveState.ObservedAt,
		})
	}
	for _, pressureState := range snapshot.Pressures {
		equipment = append(equipment, model.Equipment{
			Name:      pressureState.StationID + "/compressor",
			Kind:      "compressor",
			State:     string(pressureState.Mode),
			UpdatedAt: pressureState.ObservedAt,
		})
	}
	for _, closure := range snapshot.Closures {
		state := "unlocked"
		if closure.Locked {
			state = "locked"
		}
		equipment = append(equipment, model.Equipment{
			Name:      closure.StationID + "/closure",
			Kind:      "closure",
			State:     state,
			UpdatedAt: closure.ObservedAt,
		})
	}
	sort.Slice(equipment, func(i, j int) bool { return equipment[i].Name < equipment[j].Name })
	return equipment
}

func (r *Runtime) HealthDetails() map[string]any {
	snapshot := r.Snapshot()
	validationError := snapshot.Validate()
	sequence := int64(0)
	checksum := ""
	if r.control.Persistent != nil {
		sequence, checksum = r.control.Persistent.Tail()
	}
	ready := validationError == nil && run.Health(r.Runs) && tracker.Ready(r.Tracker) && launcher.Ready(r.Launcher)
	recoverySteps := NewRecoveryPlan().Steps()
	return map[string]any{
		"status":              map[bool]string{true: "ok", false: "degraded"}[ready],
		"ready":               ready,
		"revision":            snapshot.Revision,
		"active_runs":         len(run.Active(r.Runs)),
		"journal_events":      len(r.Journal.Events()),
		"persistent_sequence": sequence,
		"persistent_checksum": checksum,
		"validation_error":    errorText(validationError),
		"route_leases":        len(route.Snapshot(r.Routes)),
		"emergency_safe":      emergency.Safe(r.Pressure),
		"pressure_samples":    len(r.Pressure.History()),
		"recovery_steps":      recoverySteps,
		"captured_at":         snapshot.CapturedAt,
	}
}

func (r *Runtime) ReviseTracking(runID string) (int64, error) {
	if _, ok := r.Runs.Get(runID); !ok {
		return 0, fmt.Errorf("run %s not found", runID)
	}
	generation := r.Tracker.Revise(runID)
	if err := r.appendEvent(model.Event{RunID: runID, Kind: "tracking_revision", Epoch: generation}); err != nil {
		return 0, err
	}
	r.mu.Lock()
	r.control.Revision = model.NewRevision()
	r.mu.Unlock()
	return generation, nil
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
