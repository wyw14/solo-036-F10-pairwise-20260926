package permit

import (
	"sort"
	"sync"

	"github.com/wyw14/solo-036/internal/model"
)

type Evaluator struct {
	mu         sync.Mutex
	generation map[string]int64
	open       map[string]bool
}

func NewEvaluator() *Evaluator {
	return &Evaluator{generation: make(map[string]int64), open: make(map[string]bool)}
}

func (e *Evaluator) AllowRepressurization(proof model.PassageEvidence, durable bool) bool {
	if !model.ValidEvidence(proof) || !durable {
		return false
	}
	e.mu.Lock()
	e.open[proof.RunID] = true
	e.generation[proof.RunID] = proof.Generation
	e.mu.Unlock()
	return true
}
func (e *Evaluator) AllowReceive(runID string, proof model.CommandReceipt, expected int64) bool {
	if runID == "" || proof.RunID != runID || !proof.Accepted || proof.Epoch != expected {
		return false
	}
	e.mu.Lock()
	e.open[runID] = true
	e.mu.Unlock()
	return true
}
func (e *Evaluator) IsOpen(runID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.open[runID]
}

// ReplaceAll swaps the full permit table in one critical section so recovery
// never exposes a half-restored permit set if a later recovery stage fails.
func (e *Evaluator) ReplaceAll(records []model.Event) {
	open := make(map[string]bool)
	generation := make(map[string]int64)
	for _, record := range records {
		if record.Kind != "passage" || !record.Durable {
			continue
		}
		open[record.RunID] = true
		generation[record.RunID] = record.Epoch
	}
	e.mu.Lock()
	e.open = open
	e.generation = generation
	e.mu.Unlock()
}

// SnapshotEvents returns the durable passage events that currently represent
// the open permit table, in run ID order.
func (e *Evaluator) SnapshotEvents() []model.Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	runIDs := make([]string, 0, len(e.open))
	for runID, isOpen := range e.open {
		if isOpen {
			runIDs = append(runIDs, runID)
		}
	}
	sort.Strings(runIDs)
	out := make([]model.Event, 0, len(runIDs))
	for _, runID := range runIDs {
		out = append(out, model.Event{RunID: runID, Kind: "passage", Epoch: e.generation[runID], Durable: true})
	}
	return out
}
