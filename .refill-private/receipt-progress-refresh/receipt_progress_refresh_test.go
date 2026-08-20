package receipt_progress_refresh_test

import (
	"context"
	"testing"
	"time"

	"samplechain/internal/custody"
	"samplechain/internal/domain"
)

type progressLedger struct{ batch domain.CustodyBatch }

func (l *progressLedger) Get(context.Context, string) (domain.CustodyBatch, error) {
	return l.batch.Clone(), nil
}
func (*progressLedger) List(context.Context) ([]domain.CustodyBatch, error) { return nil, nil }
func (*progressLedger) Create(context.Context, domain.CustodyBatch) error   { return nil }
func (*progressLedger) Commit(context.Context, string, uint64, domain.CustodyBatch) error {
	return nil
}
func (*progressLedger) Close() error { return nil }

func TestGetRefreshesMissingReceiptProgress(t *testing.T) {
	batch, err := domain.NewBatch(domain.NewBatchInput{
		ID: "B-PROGRESS", Destination: "中心实验室", ResponsiblePerson: "甲",
		Containers: []domain.ContainerInput{{ContainerID: "C-01", SampleLabel: "样品", SealNumber: "S-01", TemperatureMinC: 2, TemperatureMaxC: 8}},
	}, time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	batch.ReceiptProgress = nil
	service, err := custody.NewService(&progressLedger{batch: batch})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	got, err := service.GetBatch(context.Background(), batch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ReceiptProgress == nil || got.ReceiptProgress.TotalCount != 1 || got.ReceiptProgress.SubmittedCount != 0 {
		t.Fatalf("查询未补齐缺失的接收进度：%+v", got.ReceiptProgress)
	}
}
