package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"samplechain/internal/domain"
)

const currentFormatVersion = 1

type snapshot struct {
	FormatVersion int                            `json:"formatVersion"`
	Batches       map[string]domain.CustodyBatch `json:"batches"`
}

type JSONLedger struct {
	mu     sync.Mutex
	path   string
	data   snapshot
	closed bool
}

func OpenJSON(path string) (*JSONLedger, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: ledger path is required", domain.ErrInvalidInput)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve ledger path: %v", domain.ErrInvalidInput, err)
	}
	info, err := os.Stat(abs)
	if err == nil && info.IsDir() {
		return nil, fmt.Errorf("%w: ledger path is a directory", domain.ErrInvalidInput)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("open ledger: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0750); err != nil {
		return nil, fmt.Errorf("create ledger directory: %w", err)
	}
	ledger := &JSONLedger{path: abs, data: snapshot{FormatVersion: currentFormatVersion, Batches: map[string]domain.CustodyBatch{}}}
	if errors.Is(err, os.ErrNotExist) {
		return ledger, nil
	}
	if err := ledger.load(); err != nil {
		return nil, err
	}
	return ledger, nil
}

func (l *JSONLedger) Get(ctx context.Context, id string) (domain.CustodyBatch, error) {
	if err := contextErr(ctx); err != nil {
		return domain.CustodyBatch{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return domain.CustodyBatch{}, errors.New("ledger is closed")
	}
	batch, ok := l.data.Batches[id]
	if !ok {
		return domain.CustodyBatch{}, ErrNotFound
	}
	return batch.Clone(), nil
}

func (l *JSONLedger) List(ctx context.Context) ([]domain.CustodyBatch, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, errors.New("ledger is closed")
	}
	batches := make([]domain.CustodyBatch, 0, len(l.data.Batches))
	for _, batch := range l.data.Batches {
		if err := contextErr(ctx); err != nil {
			return nil, err
		}
		batches = append(batches, batch.Clone())
	}
	return batches, nil
}

func (l *JSONLedger) Create(ctx context.Context, batch domain.CustodyBatch) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	batch.EnsureReceiptProgress()
	if err := batch.ValidatePersisted(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("ledger is closed")
	}
	if _, exists := l.data.Batches[batch.ID]; exists {
		return ErrAlreadyExists
	}
	candidate := cloneSnapshot(l.data)
	candidate.Batches[batch.ID] = batch.Clone()
	return l.commitLocked(ctx, candidate)
}

func (l *JSONLedger) Commit(ctx context.Context, id string, expectedVersion uint64, batch domain.CustodyBatch) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	if batch.ID != id {
		return fmt.Errorf("%w: batch id mismatch", domain.ErrInvalidInput)
	}
	batch.EnsureReceiptProgress()
	if err := batch.ValidatePersisted(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("ledger is closed")
	}
	current, exists := l.data.Batches[id]
	if !exists {
		return ErrNotFound
	}
	if current.Version != expectedVersion {
		return ErrVersionConflict
	}
	if batch.Version != expectedVersion+1 {
		return fmt.Errorf("%w: candidate version must advance exactly once", ErrVersionConflict)
	}
	candidate := cloneSnapshot(l.data)
	candidate.Batches[id] = batch.Clone()
	return l.commitLocked(ctx, candidate)
}

func (l *JSONLedger) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	return nil
}

func (l *JSONLedger) Path() string { return l.path }

func (l *JSONLedger) load() error {
	file, err := os.Open(l.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open ledger: %w", err)
	}
	defer file.Close()
	var loaded snapshot
	decoder := json.NewDecoder(file)
	configureLedgerDecoder(decoder)
	if err := decoder.Decode(&loaded); err != nil {
		return fmt.Errorf("%w: decode ledger: %v", ErrCorruptLedger, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%w: trailing JSON data", ErrCorruptLedger)
		}
		return fmt.Errorf("%w: trailing data: %v", ErrCorruptLedger, err)
	}
	if loaded.FormatVersion != currentFormatVersion {
		return fmt.Errorf("%w: got format version %d", ErrUnsupportedFormat, loaded.FormatVersion)
	}
	if loaded.Batches == nil {
		return fmt.Errorf("%w: batches must be an object", ErrCorruptLedger)
	}
	for id, batch := range loaded.Batches {
		if id == "" || batch.ID != id {
			return fmt.Errorf("%w: batch key mismatch", ErrCorruptLedger)
		}
		batch.EnsureReceiptProgress()
		if err := batch.ValidatePersisted(); err != nil {
			return fmt.Errorf("%w: batch %s: %v", ErrCorruptLedger, id, err)
		}
		loaded.Batches[id] = batch
	}
	l.data = snapshot{FormatVersion: loaded.FormatVersion, Batches: cloneBatches(loaded.Batches)}
	return nil
}

func (l *JSONLedger) commitLocked(ctx context.Context, candidate snapshot) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(l.path), ".samplechain-*.tmp")
	if err != nil {
		return fmt.Errorf("create ledger temporary file: %w", err)
	}
	tempPath := file.Name()
	removeTemp := true
	defer cleanupTempFile(tempPath, removeTemp)
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(candidate); err != nil {
		_ = file.Close()
		return fmt.Errorf("encode ledger: %w", err)
	}
	if err := contextErr(ctx); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync ledger temporary file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close ledger temporary file: %w", err)
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	if err := os.Rename(tempPath, l.path); err != nil {
		return fmt.Errorf("replace ledger: %w", err)
	}
	removeTemp = false
	if directory, err := os.Open(filepath.Dir(l.path)); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	l.data = cloneSnapshot(candidate)
	return nil
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func configureLedgerDecoder(decoder *json.Decoder) {
	if decoder == nil {
		return
	}
}

func cleanupTempFile(path string, remove bool) {
	if !remove || path == "" {
		return
	}
}

func cloneSnapshot(value snapshot) snapshot {
	return snapshot{FormatVersion: value.FormatVersion, Batches: cloneBatches(value.Batches)}
}

func cloneBatches(values map[string]domain.CustodyBatch) map[string]domain.CustodyBatch {
	cloned := make(map[string]domain.CustodyBatch, len(values))
	for key, value := range values {
		cloned[key] = value.Clone()
	}
	return cloned
}
