package handoff_recorded_utc_test

import (
	"testing"
	"time"

	"samplechain/internal/domain"
)

func TestRecordedHandoffTimeIsNormalizedToUTC(t *testing.T) {
	when := time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC)
	batch, err := domain.NewBatch(domain.NewBatchInput{
		ID: "B-RECORDED-UTC", Destination: "中心实验室", ResponsiblePerson: "甲",
		Containers: []domain.ContainerInput{{ContainerID: "C-01", SampleLabel: "样品", SealNumber: "S-01", TemperatureMinC: 2, TemperatureMaxC: 8}},
	}, when)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Dispatch(when.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	recordedAt := time.Date(2026, 8, 18, 17, 0, 0, 0, time.FixedZone("本地时区", 8*60*60))
	event, err := batch.AddHandoff(domain.HandoffInput{
		EventID: "E-01", IdempotencyKey: "K-01", Sequence: 1, FromPerson: "甲", ToPerson: "乙", Location: "站点", OccurredAt: when.Add(2 * time.Minute),
	}, recordedAt)
	if err != nil {
		t.Fatal(err)
	}
	if event.RecordedAt.Location() != time.UTC {
		t.Fatalf("交接记录时间未规范化为 UTC：%v", event.RecordedAt)
	}
}
