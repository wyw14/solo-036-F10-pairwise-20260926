package journal

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

func validSnapshotControl() model.ControlSnapshot {
	return model.ControlSnapshot{
		Revision:   "rev",
		CapturedAt: time.Now().UTC(),
		Stations: []model.Station{
			{ID: "a", Name: "A", Role: model.StationLauncher, Kilometer: 0, PressureLimit: 10, Enabled: true},
			{ID: "b", Name: "B", Role: model.StationReceiver, Kilometer: 10, PressureLimit: 10, Enabled: true},
		},
	}
}

func TestResolveSnapshotPath(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	canonical, err := store.ResolveSnapshotPath("")
	if err != nil {
		t.Fatal(err)
	}
	if canonical != store.SnapshotPath() {
		t.Fatalf("empty reference must resolve to canonical snapshot, got %s", canonical)
	}

	named, err := store.ResolveSnapshotPath("snapshot-2026.json")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(named) != store.directory {
		t.Fatalf("named snapshot must stay inside journal directory, got %s", named)
	}

	for _, reference := range []string{"../escape.json", "../../etc/passwd", "a/../../escape.json", "/etc/passwd"} {
		if _, err := store.ResolveSnapshotPath(reference); err == nil {
			t.Fatalf("reference %q must not resolve outside journal directory", reference)
		}
	}
}

func TestDecodeSnapshotIsIndependentOfTail(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	control := validSnapshotControl()
	if err := store.WriteSnapshot(control); err != nil {
		t.Fatal(err)
	}

	// A named copy decodes even though the live journal tail is unrelated to it;
	// DecodeSnapshot must not perform the tail gate that ReadSnapshot enforces.
	canonical, err := os.ReadFile(store.SnapshotPath())
	if err != nil {
		t.Fatal(err)
	}
	namedPath := filepath.Join(dir, "snapshot-named.json")
	if err := os.WriteFile(namedPath, canonical, 0o640); err != nil {
		t.Fatal(err)
	}
	resolved, err := store.ResolveSnapshotPath("snapshot-named.json")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.DecodeSnapshot(resolved)
	if err != nil {
		t.Fatalf("decode named snapshot: %v", err)
	}
	if len(snapshot.Control.Stations) != 2 {
		t.Fatalf("expected two stations, got %d", len(snapshot.Control.Stations))
	}

	if _, err := store.DecodeSnapshot(filepath.Join(dir, "missing.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected not-exist error, got %v", err)
	}

	corrupt := filepath.Join(dir, "snapshot-corrupt.json")
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DecodeSnapshot(corrupt); err == nil || !strings.Contains(err.Error(), "decode snapshot") {
		t.Fatalf("expected decode error for corrupt snapshot, got %v", err)
	}
}
