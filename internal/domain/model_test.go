package domain

import (
	"errors"
	"math"
	"testing"
	"time"
)

func testBatchInput() NewBatchInput {
	return NewBatchInput{ID: "B-001", Destination: "接收实验室", ResponsiblePerson: "协调员", Containers: []ContainerInput{
		{ContainerID: "C-01", SampleLabel: "样品一", SealNumber: "S-01", TemperatureMinC: 2, TemperatureMaxC: 8},
		{ContainerID: "C-02", SampleLabel: "样品二", SealNumber: "S-02", TemperatureMinC: -20, TemperatureMaxC: -10},
	}}
}

func TestNewBatchRejectsNormalizedDuplicatesAndNonFiniteTemperature(t *testing.T) {
	input := testBatchInput()
	input.Containers[1].ContainerID = " c-01 "
	if _, err := NewBatch(input, time.Now()); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("duplicate container id error = %v", err)
	}
	input = testBatchInput()
	input.Containers[1].SealNumber = " s-01 "
	if _, err := NewBatch(input, time.Now()); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("duplicate seal error = %v", err)
	}
	input = testBatchInput()
	input.Containers[0].TemperatureMinC = math.NaN()
	if _, err := NewBatch(input, time.Now()); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("non-finite temperature error = %v", err)
	}
}

func TestBatchLifecycleUsesClosedTemperatureIntervalAndPreservesExceptions(t *testing.T) {
	when := time.Date(2026, 8, 18, 1, 2, 3, 0, time.UTC)
	batch, err := NewBatch(testBatchInput(), when)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Dispatch(when.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.AddHandoff(HandoffInput{EventID: "E-01", IdempotencyKey: "K-01", Sequence: 1, FromPerson: "甲", ToPerson: "乙", Location: "交接点", OccurredAt: when}, when.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Receive(ReceiveInput{Results: []ReceiveResult{
		{ContainerID: "C-01", ReceivedSealNumber: "S-01", ReceivedTemperatureC: 8},
		{ContainerID: "C-02", ReceivedSealNumber: "S-X", ReceivedTemperatureC: -5, ExceptionNote: "封签不一致"},
	}}, when.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if batch.Containers[0].Condition != ConditionNormal || batch.Containers[1].Condition != ConditionException || batch.Containers[1].ReceivedSealNumber != "S-X" {
		t.Fatalf("unexpected receipt state: %+v", batch.Containers)
	}
	if err := batch.Close("R-01", when.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if batch.Status != StatusClosed || batch.Version != 5 || batch.Receipt == nil || batch.Receipt.ContainerTotals[string(ConditionException)] != 1 {
		t.Fatalf("unexpected closed state: %+v", batch)
	}
	if err := batch.ValidatePersisted(); err != nil {
		t.Fatalf("closed batch should validate: %v", err)
	}
	originalDigest := batch.Receipt.Digest
	batch.Receipt.Digest = "changed"
	if err := batch.ValidatePersisted(); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("tampered receipt should fail validation: %v", err)
	}
	batch.Receipt.Digest = originalDigest
}

func TestReceiveRejectsManifestMismatchWithoutPartialMutation(t *testing.T) {
	batch, err := NewBatch(testBatchInput(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Dispatch(time.Now()); err != nil {
		t.Fatal(err)
	}
	err = batch.Receive(ReceiveInput{Results: []ReceiveResult{{ContainerID: "C-01", ReceivedSealNumber: "S-01", ReceivedTemperatureC: 4}}}, time.Now())
	if !errors.Is(err, ErrInvalidInput) || batch.Status != StatusInTransit || batch.Containers[0].Condition != ConditionPending {
		t.Fatalf("mismatch should leave batch untouched: err=%v batch=%+v", err, batch)
	}
}
