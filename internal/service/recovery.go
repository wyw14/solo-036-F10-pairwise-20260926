package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/solo-036/internal/emergency"
	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/route"
)

func (r *Runtime) RecoveryBarrier() error {
	if err := ValidateRecoveryOrdering(); err != nil {
		return err
	}
	if !r.JournalHealthy() {
		return errors.New("journal is not healthy")
	}
	return r.validateRecoveredState()
}

// validateRecoveredState checks the state invariants that must hold after a
// recovery publish: every route lease is fenced by a live, non-terminal run,
// and no operation sits in depressurization while pressure remains. It is the
// in-memory counterpart of analyzeRecoveryEvidence's route/pressure stages and
// deliberately does not require the in-memory journal to be populated: during
// recovery the durable journal is verified directly, before any publish.
func (r *Runtime) validateRecoveredState() error {
	runs := make(map[string]model.Run)
	for _, recorded := range r.Runs.Snapshot() {
		runs[recorded.ID] = recorded
	}
	for _, lease := range route.Snapshot(r.Routes) {
		current, ok := runs[lease.RunID]
		if !ok {
			return fmt.Errorf("route %s references missing run %s", lease.Route, lease.RunID)
		}
		if current.State.Terminal() {
			return fmt.Errorf("route %s references terminal run %s", lease.Route, lease.RunID)
		}
	}
	for _, operation := range r.Operations() {
		if operation.State == model.StateDepressurizing && r.Pressure.Current() > 0 {
			incident := emergency.Incident(operation.ID, "pressure remains during depressurization")
			if emergency.IsCritical(incident) {
				return errors.New(incident.Message)
			}
		}
	}
	return nil
}

func (r *Runtime) WaitForSafeState(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := r.RecoveryBarrier(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
