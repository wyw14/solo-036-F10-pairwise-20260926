package journal

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

func NewFileStore(directory string) (*FileStore, error) {
	cleaned := filepath.Clean(directory)
	if strings.TrimSpace(directory) == "" || cleaned == "." {
		return nil, errors.New("journal directory is required")
	}
	if err := os.MkdirAll(cleaned, 0o750); err != nil {
		return nil, fmt.Errorf("create journal directory: %w", err)
	}
	store := &FileStore{
		directory:    cleaned,
		eventsPath:   filepath.Join(cleaned, "events.jsonl"),
		snapshotPath: filepath.Join(cleaned, "snapshot.json"),
	}
	if err := store.restoreTail(); err != nil {
		return nil, err
	}
	return store, nil
}

func eventChecksum(sequence int64, previous string, event model.Event) (string, error) {
	payload, err := json.Marshal(struct {
		Sequence int64       `json:"sequence"`
		Previous string      `json:"previous"`
		Event    model.Event `json:"event"`
	}{
		Sequence: sequence,
		Previous: previous,
		Event:    event,
	})
	if err != nil {
		return "", fmt.Errorf("marshal event checksum payload: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (s *FileStore) readRecords() ([]FileRecord, error) {
	file, err := os.Open(s.eventsPath)
	if errors.Is(err, os.ErrNotExist) {
		return []FileRecord{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open event log: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 4*1024*1024)
	records := make([]FileRecord, 0)
	var previous string
	var expectedSequence int64 = 1
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(line) == 0 {
			continue
		}
		var record FileRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("decode event record %d: %w", expectedSequence, err)
		}
		if record.Sequence != expectedSequence {
			return nil, fmt.Errorf("event sequence gap: got %d want %d", record.Sequence, expectedSequence)
		}
		if record.Previous != previous {
			return nil, fmt.Errorf("event chain mismatch at sequence %d", record.Sequence)
		}
		expectedChecksum, err := eventChecksum(record.Sequence, record.Previous, record.Event)
		if err != nil {
			return nil, err
		}
		if record.Checksum != expectedChecksum {
			return nil, fmt.Errorf("event checksum mismatch at sequence %d", record.Sequence)
		}
		records = append(records, record)
		previous = record.Checksum
		expectedSequence++
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan event log: %w", err)
	}
	return records, nil
}

func (s *FileStore) Append(event model.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := s.sequence + 1
	event.Durable = true
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	checksum, err := eventChecksum(next, s.checksum, event)
	if err != nil {
		return err
	}
	record := FileRecord{
		Sequence: next,
		Previous: s.checksum,
		Checksum: checksum,
		Event:    event,
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal event record: %w", err)
	}
	file, err := os.OpenFile(s.eventsPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("open event log for append: %w", err)
	}
	if _, err := file.Write(append(payload, '\n')); err != nil {
		file.Close()
		return fmt.Errorf("append event record: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync event record: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close event log: %w", err)
	}
	s.sequence = next
	s.checksum = checksum
	return nil
}

func (s *FileStore) Events() ([]model.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.readRecords()
	if err != nil {
		return nil, err
	}
	events := make([]model.Event, 0, len(records))
	for _, record := range records {
		events = append(events, record.Event)
	}
	return events, nil
}

func (s *FileStore) WriteSnapshot(control model.ControlSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := control.Validate(); err != nil {
		return fmt.Errorf("validate control snapshot: %w", err)
	}
	payload, err := json.MarshalIndent(FileSnapshot{
		Version:      1,
		WrittenAt:    time.Now().UTC(),
		LastSequence: s.sequence,
		LastChecksum: s.checksum,
		Control:      control,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal control snapshot: %w", err)
	}
	temporary, err := os.CreateTemp(s.directory, "snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("create snapshot temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	committed := false
	defer func() {
		if !committed {
			os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o640); err != nil {
		temporary.Close()
		return fmt.Errorf("set snapshot permissions: %w", err)
	}
	if _, err := temporary.Write(append(payload, '\n')); err != nil {
		temporary.Close()
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync snapshot: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close snapshot: %w", err)
	}
	if err := os.Rename(temporaryName, s.snapshotPath); err != nil {
		return fmt.Errorf("commit snapshot: %w", err)
	}
	committed = true
	return nil
}

func (s *FileStore) SnapshotPath() string { return s.snapshotPath }

// ResolveSnapshotPath maps a caller-provided snapshot reference to an absolute
// path inside the journal directory. An empty reference selects the canonical
// snapshot. References that escape the journal directory (absolute paths or
// ".." traversal) are rejected so a dry run can never read files outside the
// configured data directory.
func (s *FileStore) ResolveSnapshotPath(reference string) (string, error) {
	if strings.TrimSpace(reference) == "" {
		return s.snapshotPath, nil
	}
	if filepath.IsAbs(reference) {
		cleaned := filepath.Clean(reference)
		if !isWithin(s.directory, cleaned) {
			return "", fmt.Errorf("snapshot %q is outside journal directory", reference)
		}
		return cleaned, nil
	}
	cleaned := filepath.Clean(filepath.Join(s.directory, reference))
	if !isWithin(s.directory, cleaned) {
		return "", fmt.Errorf("snapshot %q escapes journal directory", reference)
	}
	return cleaned, nil
}

func isWithin(base, target string) bool {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

// DecodeSnapshot reads, decodes and structurally validates a snapshot file
// without touching runtime journal state. It deliberately does not compare the
// snapshot tail against the on-disk log, so a read-only dry run can inspect a
// snapshot even before it becomes recoverable. The file is opened read-only.
func (s *FileStore) DecodeSnapshot(path string) (FileSnapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return FileSnapshot{}, fmt.Errorf("open snapshot: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 16*1024*1024))
	decoder.DisallowUnknownFields()
	var snapshot FileSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return FileSnapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	if snapshot.Version != 1 {
		return FileSnapshot{}, fmt.Errorf("unsupported snapshot version %d", snapshot.Version)
	}
	if err := snapshot.Control.Validate(); err != nil {
		return FileSnapshot{}, fmt.Errorf("validate snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *FileStore) ReadSnapshot() (FileSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot, err := s.DecodeSnapshot(s.snapshotPath)
	if err != nil {
		return FileSnapshot{}, err
	}
	if snapshot.LastSequence > s.sequence {
		return FileSnapshot{}, errors.New("snapshot references unavailable events")
	}
	return snapshot, nil
}

func (s *FileStore) Verify() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.readRecords()
	if err != nil {
		return err
	}
	if len(records) == 0 {
		if s.sequence != 0 || s.checksum != "" {
			return errors.New("empty journal has a non-empty tail")
		}
		return nil
	}
	last := records[len(records)-1]
	if last.Sequence != s.sequence || last.Checksum != s.checksum {
		return errors.New("journal tail does not match durable log")
	}
	return nil
}

func (s *FileStore) Tail() (int64, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sequence, s.checksum
}
