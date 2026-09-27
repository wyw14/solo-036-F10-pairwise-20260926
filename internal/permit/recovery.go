package permit

import "github.com/wyw14/solo-036/internal/model"

// Restore merges durable passage events into the evaluator. Kept for
// compatibility; recovery now prefers ReplaceAll so the permit table is
// published as one consistent snapshot.
func Restore(e *Evaluator, records []model.Event) {
	for _, r := range records {
		if r.Kind == "passage" && r.Durable {
			e.mu.Lock()
			e.open[r.RunID] = true
			e.generation[r.RunID] = r.Epoch
			e.mu.Unlock()
		}
	}
}
func Snapshot(e *Evaluator) map[string]bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]bool)
	for k, v := range e.open {
		out[k] = v
	}
	return out
}
