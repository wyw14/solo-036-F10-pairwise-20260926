package route

import (
	"fmt"
	"github.com/wyw14/solo-036/internal/model"
	"time"
)

func (c *Coordinator) Reserve(route, runID string, epoch int64) (model.RouteLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if owner, ok := c.owners[route]; ok && owner.RunID != runID {
		return model.RouteLease{}, fmt.Errorf("route owned by %s", owner.RunID)
	}
	lease := model.RouteLease{Route: route, RunID: runID, Epoch: epoch, AcquiredAt: time.Now().UTC()}
	c.owners[route] = lease
	return lease, nil
}
func (c *Coordinator) Release(route, runID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if owner, ok := c.owners[route]; ok && owner.RunID == runID {
		delete(c.owners, route)
		delete(c.progress, runID)
	}
}

// ReplaceAll swaps the full set of route leases in one critical section and
// drops any route progress that does not belong to a surviving owner, so
// recovery publishes a consistent lease table rather than incrementally
// mutating the live one.
func (c *Coordinator) ReplaceAll(leases []model.RouteLease) {
	owners := make(map[string]model.RouteLease, len(leases))
	progress := make(map[string]float64, len(leases))
	c.mu.Lock()
	for _, lease := range leases {
		if lease.Route == "" || lease.RunID == "" {
			continue
		}
		owners[lease.Route] = lease
		if value, ok := c.progress[lease.RunID]; ok {
			progress[lease.RunID] = value
		}
	}
	c.owners = owners
	c.progress = progress
	c.mu.Unlock()
}
