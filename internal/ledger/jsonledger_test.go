package ledger

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"samplechain/internal/domain"
)

func ledgerBatch(t *testing.T) domain.CustodyBatch {
	t.Helper()
	batch, err := domain.NewBatch(domain.NewBatchInput{ID: "B-001", Destination: "实验室", ResponsiblePerson: "甲", Containers: []domain.ContainerInput{{ContainerID: "C-01", SampleLabel: "样品", SealNumber: "S-01", TemperatureMinC: 0, TemperatureMaxC: 10}}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func TestJSONLedgerPersistsAndReloadsAtomicSnapshots(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "ledger.json")
	store, err := OpenJSON(path)
	if err != nil {
		t.Fatal(err)
	}
	batch := ledgerBatch(t)
	if err := store.Create(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	updated := batch.Clone()
	if err := updated.Dispatch(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(context.Background(), batch.ID, batch.Version, updated); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reloaded, err := OpenJSON(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	got, err := reloaded.Get(context.Background(), batch.ID)
	if err != nil || got.Status != domain.StatusInTransit || got.Version != 2 {
		t.Fatalf("reloaded batch = %+v, err = %v", got, err)
	}
	got.Containers[0].ContainerID = "changed"
	again, err := reloaded.Get(context.Background(), batch.ID)
	if err != nil || again.Containers[0].ContainerID == "changed" {
		t.Fatalf("ledger returned an aliased batch: %+v, err=%v", again, err)
	}
}

func TestJSONLedgerRejectsCorruptionAndCanceledCommitKeepsSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	store, err := OpenJSON(path)
	if err != nil {
		t.Fatal(err)
	}
	batch := ledgerBatch(t)
	if err := store.Create(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	updated := batch.Clone()
	if err := updated.Dispatch(time.Now()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Commit(ctx, batch.ID, batch.Version, updated); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled commit error = %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(before) != string(after) {
		t.Fatalf("canceled commit changed file: err=%v", err)
	}
	store.Close()
	if err := os.WriteFile(path, []byte("{\"formatVersion\":1,\"batches\":"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJSON(path); !errors.Is(err, ErrCorruptLedger) {
		t.Fatalf("corruption error = %v", err)
	}
}

func TestJSONLedgerDistinguishesUnsupportedFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	if err := os.WriteFile(path, []byte(`{"formatVersion":99,"batches":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJSON(path); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("unsupported format error = %v", err)
	}
}
