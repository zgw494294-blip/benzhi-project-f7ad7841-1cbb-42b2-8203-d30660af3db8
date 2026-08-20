package list_context_cancel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"samplechain/internal/custody"
	"samplechain/internal/domain"
)

type listLedger struct{ batch domain.CustodyBatch }

func (l *listLedger) Get(context.Context, string) (domain.CustodyBatch, error) {
	return l.batch.Clone(), nil
}
func (l *listLedger) List(context.Context) ([]domain.CustodyBatch, error) {
	return []domain.CustodyBatch{l.batch.Clone()}, nil
}
func (*listLedger) Create(context.Context, domain.CustodyBatch) error { return nil }
func (*listLedger) Commit(context.Context, string, uint64, domain.CustodyBatch) error {
	return nil
}
func (*listLedger) Close() error { return nil }

func TestListHonorsCanceledContextAfterLedgerRead(t *testing.T) {
	batch, err := domain.NewBatch(domain.NewBatchInput{
		ID: "B-LIST", Destination: "中心实验室", ResponsiblePerson: "甲",
		Containers: []domain.ContainerInput{{ContainerID: "C-01", SampleLabel: "样品", SealNumber: "S-01", TemperatureMinC: 2, TemperatureMaxC: 8}},
	}, time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	service, err := custody.NewService(&listLedger{batch: batch})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = service.ListBatches(ctx, domain.BatchListQuery{Limit: 20})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("账本读取后取消的上下文未被传播：%v", err)
	}
}
