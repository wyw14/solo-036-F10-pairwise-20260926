package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"
)

func (h *Handler) PersistSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := h.runtime.PersistSnapshot(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "persisted"})
}

func (h *Handler) RecoveryDryRun(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.runtime.DryRunRecovery())
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
