package degenerate_temperature_test

import (
	"testing"
	"time"

	"samplechain/internal/domain"
)

func TestClosedTemperaturePointAcceptsExactMeasurement(t *testing.T) {
	when := time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC)
	batch, err := domain.NewBatch(domain.NewBatchInput{
		ID: "B-POINT", Destination: "中心实验室", ResponsiblePerson: "甲",
		Containers: []domain.ContainerInput{{ContainerID: "C-01", SampleLabel: "样品", SealNumber: "S-01", TemperatureMinC: 5, TemperatureMaxC: 5}},
	}, when)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Dispatch(when.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Receive(domain.ReceiveInput{Results: []domain.ReceiveResult{{ContainerID: "C-01", ReceivedSealNumber: "S-01", ReceivedTemperatureC: 5}}}, when.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if batch.Containers[0].Condition != domain.ConditionNormal {
		t.Fatalf("闭区间退化为单点时的精确温度未判定为正常：%+v", batch.Containers[0])
	}
}
