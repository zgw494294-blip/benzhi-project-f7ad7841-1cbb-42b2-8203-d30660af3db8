package manifest_key_normalization_test

import (
	"errors"
	"testing"
	"time"

	"samplechain/internal/domain"
)

func TestManifestRejectsCaseFoldedDuplicateContainerID(t *testing.T) {
	_, err := domain.NewBatch(domain.NewBatchInput{
		ID: "B-NORMALIZE", Destination: "中心实验室", ResponsiblePerson: "甲",
		Containers: []domain.ContainerInput{
			{ContainerID: "C-S1", SampleLabel: "样品一", SealNumber: "S-01", TemperatureMinC: 2, TemperatureMaxC: 8},
			{ContainerID: "C-ſ1", SampleLabel: "样品二", SealNumber: "S-02", TemperatureMinC: 2, TemperatureMaxC: 8},
		},
	}, time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC))
	if !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("规范化后重复的容器编号未被拒绝：%v", err)
	}
}
