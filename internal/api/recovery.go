package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/wyw14/solo-036/internal/service"
)

func (h *Handler) PersistSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := h.runtime.PersistSnapshot(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "persisted"})
}

func (h *Handler) RecoveryBarrier(w http.ResponseWriter, r *http.Request) {
	timeout := 2 * time.Second
	if value := r.URL.Query().Get("timeout_ms"); value != "" {
		milliseconds, err := strconv.Atoi(value)
		if err != nil || milliseconds < 1 || milliseconds > 30000 {
			writeError(w, r, http.StatusBadRequest, errors.New("timeout_ms must be between 1 and 30000"))
			return
		}
		timeout = time.Duration(milliseconds) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	if err := h.runtime.WaitForSafeState(ctx, 20*time.Millisecond); err != nil {
		writeError(w, r, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "safe", "checked_at": time.Now().UTC()})
}

// RecoveryDryRun answers "if I restarted and recovered this snapshot right now,
// what would come back and where would recovery get stuck?" It is strictly
// read-only: the runtime state is never modified. A snapshot whose evidence
// blocks recovery still returns 200 with ready=false and per-stage blocking
// reasons, so operators can inspect the verdict; only access/decoding failures
// produce error statuses.
func (h *Handler) RecoveryDryRun(w http.ResponseWriter, r *http.Request) {
	snapshot := strings.TrimSpace(r.URL.Query().Get("snapshot"))
	if len(snapshot) > 4096 {
		writeError(w, r, http.StatusBadRequest, errors.New("snapshot reference is too long"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	report, err := h.runtime.DryRunRecovery(ctx, snapshot)
	if err != nil {
		var snapshotErr *service.SnapshotError
		switch {
		case errors.Is(err, service.ErrRecoveryPersistenceNotConfigured):
			writeError(w, r, http.StatusServiceUnavailable, err)
		case errors.As(err, &snapshotErr):
			if snapshotErr.Kind == service.SnapshotNotFound {
				writeError(w, r, http.StatusNotFound, err)
			} else {
				writeError(w, r, http.StatusUnprocessableEntity, err)
			}
		default:
			writeError(w, r, http.StatusInternalServerError, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, report)
}
