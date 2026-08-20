package receive_atomicity_test

import (
	"errors"
	"testing"
	"time"

	"samplechain/internal/domain"
)

func TestReceiveValidationLeavesEarlierContainersUntouched(t *testing.T) {
	when := time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC)
	batch, err := domain.NewBatch(domain.NewBatchInput{
		ID: "B-ATOMIC", Destination: "中心实验室", ResponsiblePerson: "甲",
		Containers: []domain.ContainerInput{
			{ContainerID: "C-01", SampleLabel: "样品一", SealNumber: "S-01", TemperatureMinC: 2, TemperatureMaxC: 8},
			{ContainerID: "C-02", SampleLabel: "样品二", SealNumber: "S-02", TemperatureMinC: 2, TemperatureMaxC: 8},
		},
	}, when)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Dispatch(when.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	version := batch.Version
	err = batch.Receive(domain.ReceiveInput{Results: []domain.ReceiveResult{
		{ContainerID: "C-01", ReceivedSealNumber: "S-01", ReceivedTemperatureC: 4},
		{ContainerID: "C-02", ReceivedSealNumber: "", ReceivedTemperatureC: 4},
	}}, when.Add(2*time.Minute))
	if !errors.Is(err, domain.ErrInvalidInput) || batch.Version != version || batch.Status != domain.StatusInTransit || batch.Containers[0].Condition != domain.ConditionPending {
		t.Fatalf("接收校验失败后仍留下部分结果：err=%v batch=%+v", err, batch)
	}
}
