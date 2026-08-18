package custody

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"samplechain/internal/domain"
	"samplechain/internal/ledger"
)

func newTestService(t *testing.T) (*Service, func()) {
	t.Helper()
	store, err := ledger.OpenJSON(filepath.Join(t.TempDir(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(store)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	return service, func() { _ = service.Close() }
}

func createTestBatch(t *testing.T, service *Service) domain.CustodyBatch {
	t.Helper()
	batch, err := service.CreateBatch(context.Background(), domain.NewBatchInput{ID: "B-001", Destination: "实验室", ResponsiblePerson: "甲", Containers: []domain.ContainerInput{{ContainerID: "C-01", SampleLabel: "样品", SealNumber: "S-01", TemperatureMinC: 2, TemperatureMaxC: 8}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func TestServiceEnforcesVersionAndIdempotency(t *testing.T) {
	service, cleanup := newTestService(t)
	defer cleanup()
	batch := createTestBatch(t, service)
	if _, err := service.Dispatch(context.Background(), batch.ID, 0); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale dispatch error = %v", err)
	}
	batch, err := service.Dispatch(context.Background(), batch.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Now().UTC().Truncate(time.Second)
	input := domain.HandoffInput{EventID: "E-01", IdempotencyKey: "K-01", Sequence: 1, FromPerson: "甲", ToPerson: "乙", Location: "站点", OccurredAt: when}
	batch, err = service.AddHandoff(context.Background(), batch.ID, batch.Version, input)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := service.AddHandoff(context.Background(), batch.ID, 999, input)
	if err != nil || retry.Version != batch.Version || len(retry.Handoffs) != 1 {
		t.Fatalf("idempotent retry = %+v, err=%v", retry, err)
	}
	input.ToPerson = "丙"
	if _, err := service.AddHandoff(context.Background(), batch.ID, batch.Version, input); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different idempotent content error = %v", err)
	}
}

func TestServiceCancellationDoesNotCommit(t *testing.T) {
	service, cleanup := newTestService(t)
	defer cleanup()
	batch := createTestBatch(t, service)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Dispatch(ctx, batch.ID, batch.Version); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled dispatch error = %v", err)
	}
	got, err := service.GetBatch(context.Background(), batch.ID)
	if err != nil || got.Status != domain.StatusDraft || got.Version != batch.Version {
		t.Fatalf("canceled dispatch changed state: %+v, err=%v", got, err)
	}
}
