package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/wyw14/solo-036/internal/service"
)

// newPersistentServer configures persistence, drives one real launch through
// the public API so the event journal and snapshot agree, and persists it.
func newPersistentServer(t *testing.T, dir string) http.Handler {
	t.Helper()
	runtime := service.NewRuntime()
	if err := runtime.ConfigurePersistence(dir); err != nil {
		t.Fatal(err)
	}
	request := service.LaunchRequest{
		StationID:      "station-a",
		RouteID:        "main-station-a-station-c",
		TargetPressure: 8.0,
	}
	if _, err := runtime.Launch(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := runtime.PersistSnapshot(); err != nil {
		t.Fatal(err)
	}
	return NewRouter(runtime)
}

func TestRecoveryDryRunEndpointNotConfigured(t *testing.T) {
	server := NewRouter(service.NewRuntime())
	request := httptest.NewRequest(http.MethodGet, "/api/recovery/dry-run", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 without persistence, got %d: %s", response.Code, response.Body.String())
	}
}

func TestRecoveryDryRunEndpointOK(t *testing.T) {
	server := newPersistentServer(t, t.TempDir())
	request := httptest.NewRequest(http.MethodGet, "/api/recovery/dry-run", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	var report map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report["mode"] != "dry_run" || report["mutated"] != false {
		t.Fatalf("unexpected report: %v", report)
	}
	if report["ready"] != true {
		t.Fatalf("expected ready verdict, got %v", report)
	}
	evidence, ok := report["evidence"].(map[string]any)
	if !ok {
		t.Fatalf("evidence section missing: %v", report)
	}
	if runs, ok := evidence["runs"].([]any); !ok || len(runs) != 1 {
		t.Fatalf("expected one recoverable run listed, got %v", evidence["runs"])
	}
	if routes, ok := evidence["routes"].([]any); !ok || len(routes) != 1 {
		t.Fatalf("expected one recoverable route listed, got %v", evidence["routes"])
	}
}

func TestRecoveryDryRunEndpointMissingSnapshot(t *testing.T) {
	server := newPersistentServer(t, t.TempDir())
	request := httptest.NewRequest(http.MethodGet, "/api/recovery/dry-run?snapshot=absent.json", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing snapshot, got %d: %s", response.Code, response.Body.String())
	}
}

func TestRecoveryDryRunEndpointPathTraversal(t *testing.T) {
	server := newPersistentServer(t, t.TempDir())
	request := httptest.NewRequest(http.MethodGet, "/api/recovery/dry-run?snapshot=../../etc/passwd", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for path traversal, got %d", response.Code)
	}
}

func TestRecoveryDryRunEndpointBlockedVerdict(t *testing.T) {
	dir := t.TempDir()
	server := newPersistentServer(t, dir)
	// Remove the event log: the snapshot tail now points at unavailable events,
	// but the endpoint must still return 200 with ready=false rather than an
	// error, since this is a readable verdict about the snapshot.
	if err := os.Remove(filepath.Join(dir, "events.jsonl")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/recovery/dry-run", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200 verdict for blocked evidence, got %d: %s", response.Code, response.Body.String())
	}
	var report map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report["ready"] != false || report["blocked_stage"] != "journal" {
		t.Fatalf("expected blocked journal verdict, got %v", report)
	}
}
