package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode"
)

type BatchStatus string

const (
	StatusDraft     BatchStatus = "DRAFT"
	StatusInTransit BatchStatus = "IN_TRANSIT"
	StatusReceived  BatchStatus = "RECEIVED"
	StatusClosed    BatchStatus = "CLOSED"
)

type LifecycleEventType string

const (
	LifecycleCreated         LifecycleEventType = "CREATED"
	LifecycleManifestUpdated LifecycleEventType = "MANIFEST_UPDATED"
	LifecycleDispatched      LifecycleEventType = "DISPATCHED"
	LifecycleReceived        LifecycleEventType = "RECEIVED"
	LifecycleClosed          LifecycleEventType = "CLOSED"
)

type ReceiptCondition string

const (
	ConditionPending   ReceiptCondition = "PENDING"
	ConditionNormal    ReceiptCondition = "NORMAL"
	ConditionException ReceiptCondition = "EXCEPTION"
)

type CustodyBatch struct {
	ID                string           `json:"id"`
	Destination       string           `json:"destination"`
	ResponsiblePerson string           `json:"responsiblePerson"`
	Status            BatchStatus      `json:"status"`
	Containers        []Container      `json:"containers"`
	Handoffs          []HandoffEvent   `json:"handoffs"`
	LifecycleEvents   []LifecycleEvent `json:"lifecycleEvents"`
	Receipt           *CustodyReceipt  `json:"receipt,omitempty"`
	ReceiptProgress   *ReceiptProgress `json:"receiptProgress,omitempty"`
	Version           uint64           `json:"version"`
	CreatedAt         time.Time        `json:"createdAt"`
	UpdatedAt         time.Time        `json:"updatedAt"`
}

type Container struct {
	ContainerID          string           `json:"containerID"`
	SampleLabel          string           `json:"sampleLabel"`
	SealNumber           string           `json:"sealNumber"`
	TemperatureMinC      float64          `json:"temperatureMinC"`
	TemperatureMaxC      float64          `json:"temperatureMaxC"`
	ReceivedSealNumber   string           `json:"receivedSealNumber,omitempty"`
	ReceivedTemperatureC float64          `json:"receivedTemperatureC"`
	Condition            ReceiptCondition `json:"condition"`
	ExceptionNote        string           `json:"exceptionNote,omitempty"`
	ReceiptStaged        bool             `json:"receiptStaged,omitempty"`
}

type HandoffEvent struct {
	EventID        string    `json:"eventID"`
	IdempotencyKey string    `json:"idempotencyKey"`
	Sequence       uint64    `json:"sequence"`
	FromPerson     string    `json:"fromPerson"`
	ToPerson       string    `json:"toPerson"`
	Location       string    `json:"location"`
	OccurredAt     time.Time `json:"occurredAt"`
	RecordedAt     time.Time `json:"recordedAt"`
}

type LifecycleEvent struct {
	Sequence   uint64             `json:"sequence"`
	Type       LifecycleEventType `json:"type"`
	Version    uint64             `json:"version"`
	OccurredAt time.Time          `json:"occurredAt"`
}

type CustodyReceipt struct {
	ReceiptID       string         `json:"receiptID"`
	BatchID         string         `json:"batchID"`
	ClosedAt        time.Time      `json:"closedAt"`
	ContainerTotals map[string]int `json:"containerTotals"`
	HandoffCount    int            `json:"handoffCount"`
	FinalVersion    uint64         `json:"finalVersion"`
	Digest          string         `json:"digest"`
}

type ReceiptProgress struct {
	SubmittedCount      int      `json:"submittedCount"`
	TotalCount          int      `json:"totalCount"`
	PendingContainerIDs []string `json:"pendingContainerIDs"`
}

type NewBatchInput struct {
	ID                string
	Destination       string
	ResponsiblePerson string
	Containers        []ContainerInput
}

type ContainerInput struct {
	ContainerID     string
	SampleLabel     string
	SealNumber      string
	TemperatureMinC float64
	TemperatureMaxC float64
}

type HandoffInput struct {
	EventID        string
	IdempotencyKey string
	Sequence       uint64
	FromPerson     string
	ToPerson       string
	Location       string
	OccurredAt     time.Time
}

type ReceiveInput struct {
	Results []ReceiveResult
}

type ReceiveResult struct {
	ContainerID          string
	ReceivedSealNumber   string
	ReceivedTemperatureC float64
	ExceptionNote        string
}

type ManifestInput struct {
	Destination       string
	ResponsiblePerson string
	Containers        []ContainerInput
}

type BatchListQuery struct {
	Status      *BatchStatus
	Destination string
	Condition   *ReceiptCondition
	Limit       int
	Cursor      string
}

type BatchSummary struct {
	ID                string          `json:"id"`
	Destination       string          `json:"destination"`
	ResponsiblePerson string          `json:"responsiblePerson"`
	Status            BatchStatus     `json:"status"`
	Version           uint64          `json:"version"`
	UpdatedAt         time.Time       `json:"updatedAt"`
	ContainerCount    int             `json:"containerCount"`
	NormalCount       int             `json:"normalCount"`
	ExceptionCount    int             `json:"exceptionCount"`
	ReceiptProgress   ReceiptProgress `json:"receiptProgress"`
}

type BatchListTotals struct {
	BatchCount      int                      `json:"batchCount"`
	ContainerCount  int                      `json:"containerCount"`
	HandoffCount    int                      `json:"handoffCount"`
	StatusCounts    map[BatchStatus]int      `json:"statusCounts"`
	ConditionCounts map[ReceiptCondition]int `json:"conditionCounts"`
}

type BatchListResult struct {
	Items      []BatchSummary  `json:"items"`
	NextCursor string          `json:"nextCursor"`
	Totals     BatchListTotals `json:"totals"`
}

type ReceiptVerificationChecks struct {
	BatchID         bool `json:"batchID"`
	FinalVersion    bool `json:"finalVersion"`
	HandoffCount    bool `json:"handoffCount"`
	ContainerTotals bool `json:"containerTotals"`
	Digest          bool `json:"digest"`
}

type ReceiptVerification struct {
	ReceiptID string                    `json:"receiptID"`
	Digest    string                    `json:"digest"`
	Valid     bool                      `json:"valid"`
	Checks    ReceiptVerificationChecks `json:"checks"`
}

const MaxBatchListLimit = 100

func NewBatch(input NewBatchInput, now time.Time) (CustodyBatch, error) {
	if err := validateText(input.ID, "batch id"); err != nil {
		return CustodyBatch{}, err
	}
	destination, responsiblePerson, containers, err := buildManifest(ManifestInput{
		Destination: input.Destination, ResponsiblePerson: input.ResponsiblePerson, Containers: input.Containers,
	})
	if err != nil {
		return CustodyBatch{}, err
	}
	createdAt := now.UTC()
	batch := CustodyBatch{
		ID: strings.TrimSpace(input.ID), Destination: destination, ResponsiblePerson: responsiblePerson,
		Status: StatusDraft, Containers: containers, Handoffs: []HandoffEvent{}, LifecycleEvents: []LifecycleEvent{}, Version: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	if err := batch.validateLifecycleAppend(LifecycleCreated, batch.Version, createdAt); err != nil {
		return CustodyBatch{}, err
	}
	batch.recordLifecycleEvent(LifecycleCreated, createdAt)
	batch.RefreshReceiptProgress()
	return batch, nil
}

func buildManifest(input ManifestInput) (string, string, []Container, error) {
	if err := validateText(input.Destination, "destination"); err != nil {
		return "", "", nil, err
	}
	if err := validateText(input.ResponsiblePerson, "responsible person"); err != nil {
		return "", "", nil, err
	}
	if len(input.Containers) == 0 {
		return "", "", nil, fmt.Errorf("%w: at least one container is required", ErrInvalidInput)
	}
	containers := make([]Container, 0, len(input.Containers))
	seenIDs := make(map[string]struct{}, len(input.Containers))
	seenSeals := make(map[string]struct{}, len(input.Containers))
	for _, item := range input.Containers {
		id := strings.TrimSpace(item.ContainerID)
		seal := strings.TrimSpace(item.SealNumber)
		if err := validateText(id, "container id"); err != nil {
			return "", "", nil, err
		}
		if err := validateText(item.SampleLabel, "sample label"); err != nil {
			return "", "", nil, err
		}
		if err := validateText(seal, "seal number"); err != nil {
			return "", "", nil, err
		}
		if _, exists := seenIDs[manifestKey(id)]; exists {
			return "", "", nil, fmt.Errorf("%w: duplicate container id", ErrInvalidInput)
		}
		if _, exists := seenSeals[manifestKey(seal)]; exists {
			return "", "", nil, fmt.Errorf("%w: duplicate seal number", ErrInvalidInput)
		}
		if !finite(item.TemperatureMinC) || !finite(item.TemperatureMaxC) || item.TemperatureMinC > item.TemperatureMaxC {
			return "", "", nil, fmt.Errorf("%w: invalid temperature range", ErrInvalidInput)
		}
		seenIDs[manifestKey(id)] = struct{}{}
		seenSeals[manifestKey(seal)] = struct{}{}
		containers = append(containers, Container{
			ContainerID: id, SampleLabel: strings.TrimSpace(item.SampleLabel), SealNumber: seal,
			TemperatureMinC: item.TemperatureMinC, TemperatureMaxC: item.TemperatureMaxC,
			Condition: ConditionPending,
		})
	}
	return strings.TrimSpace(input.Destination), strings.TrimSpace(input.ResponsiblePerson), containers, nil
}

func (b *CustodyBatch) ReplaceDraftManifest(input ManifestInput, now time.Time) error {
	if b.Status != StatusDraft {
		return fmt.Errorf("%w: only DRAFT batches can update the manifest", ErrInvalidTransition)
	}
	destination, responsiblePerson, containers, err := buildManifest(input)
	if err != nil {
		return err
	}
	now = now.UTC()
	if err := b.validateLifecycleAppend(LifecycleManifestUpdated, b.Version+1, now); err != nil {
		return err
	}
	b.Destination = destination
	b.ResponsiblePerson = responsiblePerson
	b.Containers = containers
	b.Version++
	b.UpdatedAt = now
	b.recordLifecycleEvent(LifecycleManifestUpdated, now)
	b.RefreshReceiptProgress()
	return nil
}

func (b *CustodyBatch) Dispatch(now time.Time) error {
	if b.Status != StatusDraft {
		return fmt.Errorf("%w: only DRAFT batches can be dispatched", ErrInvalidTransition)
	}
	if len(b.Containers) == 0 {
		return fmt.Errorf("%w: empty container list", ErrInvalidInput)
	}
	for _, item := range b.Containers {
		if strings.TrimSpace(item.SealNumber) == "" {
			return fmt.Errorf("%w: every container needs a seal", ErrInvalidInput)
		}
	}
	now = now.UTC()
	if err := b.validateLifecycleAppend(LifecycleDispatched, b.Version+1, now); err != nil {
		return err
	}
	b.Status = StatusInTransit
	b.Version++
	b.UpdatedAt = now
	b.recordLifecycleEvent(LifecycleDispatched, now)
	return nil
}

func (b *CustodyBatch) AddHandoff(input HandoffInput, now time.Time) (HandoffEvent, error) {
	if b.Status != StatusInTransit {
		return HandoffEvent{}, fmt.Errorf("%w: handoffs require IN_TRANSIT status", ErrInvalidTransition)
	}
	if err := validateHandoffInput(input); err != nil {
		return HandoffEvent{}, err
	}
	if input.Sequence != uint64(len(b.Handoffs)+1) {
		return HandoffEvent{}, fmt.Errorf("%w: expected handoff sequence %d", ErrInvalidInput, len(b.Handoffs)+1)
	}
	for _, existing := range b.Handoffs {
		if strings.EqualFold(strings.TrimSpace(existing.EventID), strings.TrimSpace(input.EventID)) {
			return HandoffEvent{}, fmt.Errorf("%w: duplicate event id", ErrInvalidInput)
		}
	}
	if len(b.Handoffs) > 0 {
		previous := b.Handoffs[len(b.Handoffs)-1]
		if normalizePerson(input.FromPerson) != normalizePerson(previous.ToPerson) {
			return HandoffEvent{}, fmt.Errorf("%w: handoff responsibility chain is broken", ErrInvalidInput)
		}
		if input.OccurredAt.UTC().Before(previous.OccurredAt.UTC()) {
			return HandoffEvent{}, fmt.Errorf("%w: handoff occurred time moved backwards", ErrInvalidInput)
		}
	}
	event := HandoffEvent{
		EventID: strings.TrimSpace(input.EventID), IdempotencyKey: strings.TrimSpace(input.IdempotencyKey), Sequence: input.Sequence,
		FromPerson: strings.TrimSpace(input.FromPerson), ToPerson: strings.TrimSpace(input.ToPerson), Location: strings.TrimSpace(input.Location),
		OccurredAt: input.OccurredAt.UTC(), RecordedAt: normalizeRecordedAt(now),
	}
	b.Handoffs = append(b.Handoffs, event)
	b.Version++
	b.UpdatedAt = now.UTC()
	return event, nil
}

func (b CustodyBatch) Summary() BatchSummary {
	summary := BatchSummary{
		ID: b.ID, Destination: b.Destination, ResponsiblePerson: b.ResponsiblePerson,
		Status: b.Status, Version: b.Version, UpdatedAt: b.UpdatedAt,
		ContainerCount:  len(b.Containers),
		ReceiptProgress: b.Progress(),
	}
	for _, item := range b.Containers {
		switch item.Condition {
		case ConditionNormal:
			summary.NormalCount++
		case ConditionException:
			summary.ExceptionCount++
		}
	}
	return summary
}

func (b CustodyBatch) Progress() ReceiptProgress {
	progress := ReceiptProgress{TotalCount: len(b.Containers), PendingContainerIDs: make([]string, 0, len(b.Containers))}
	for _, item := range b.Containers {
		if item.ReceiptStaged || item.Condition == ConditionNormal || item.Condition == ConditionException {
			progress.SubmittedCount++
			continue
		}
		progress.PendingContainerIDs = append(progress.PendingContainerIDs, item.ContainerID)
	}
	return progress
}

func (b *CustodyBatch) RefreshReceiptProgress() {
	progress := b.Progress()
	b.ReceiptProgress = &progress
}

func (b *CustodyBatch) EnsureReceiptProgress() {
	if b.ReceiptProgress == nil {
		b.RefreshReceiptProgress()
	}
}

func NewBatchListTotals() BatchListTotals {
	return BatchListTotals{
		StatusCounts: map[BatchStatus]int{
			StatusDraft: 0, StatusInTransit: 0, StatusReceived: 0, StatusClosed: 0,
		},
		ConditionCounts: map[ReceiptCondition]int{
			ConditionPending: 0, ConditionNormal: 0, ConditionException: 0,
		},
	}
}

func (totals *BatchListTotals) Add(batch CustodyBatch) {
	totals.BatchCount++
	totals.ContainerCount += len(batch.Containers)
	totals.HandoffCount += len(batch.Handoffs)
	totals.StatusCounts[batch.Status]++
	for _, item := range batch.Containers {
		totals.ConditionCounts[item.Condition]++
	}
}

func (b CustodyBatch) MatchesQuery(query BatchListQuery) bool {
	if query.Status != nil && b.Status != *query.Status {
		return false
	}
	if query.Destination != "" && b.Destination != query.Destination {
		return false
	}
	if query.Condition != nil {
		for _, item := range b.Containers {
			if item.Condition == *query.Condition {
				return true
			}
		}
		return false
	}
	return true
}

func (q BatchListQuery) Validate() error {
	if q.Limit < 1 || q.Limit > MaxBatchListLimit {
		return fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, MaxBatchListLimit)
	}
	if q.Destination != "" && strings.ContainsRune(q.Destination, '\x00') {
		return fmt.Errorf("%w: destination is invalid", ErrInvalidQuery)
	}
	if q.Status != nil && !validStatus(*q.Status) {
		return fmt.Errorf("%w: status is invalid", ErrInvalidQuery)
	}
	if q.Condition != nil && !validCondition(*q.Condition) {
		return fmt.Errorf("%w: condition is invalid", ErrInvalidQuery)
	}
	return nil
}

func (b CustodyBatch) VerifyReceipt() (ReceiptVerification, error) {
	if b.Status != StatusClosed {
		return ReceiptVerification{}, fmt.Errorf("%w: only CLOSED batches have a receipt to verify", ErrInvalidTransition)
	}
	verification := ReceiptVerification{}
	if b.Receipt == nil {
		return verification, nil
	}
	verification.ReceiptID = b.Receipt.ReceiptID
	verification.Digest = b.Receipt.Digest
	totals, complete := b.containerTotals()
	checks := ReceiptVerificationChecks{
		BatchID:         b.Receipt.BatchID == b.ID,
		FinalVersion:    b.Receipt.FinalVersion == b.Version,
		HandoffCount:    b.Receipt.HandoffCount == len(b.Handoffs),
		ContainerTotals: complete && equalTotals(b.Receipt.ContainerTotals, totals),
		Digest:          b.Receipt.Digest != "" && b.Receipt.Digest == b.DigestMaterial(),
	}
	verification.Checks = checks
	verification.Valid = checks.BatchID && checks.FinalVersion && checks.HandoffCount && checks.ContainerTotals && checks.Digest
	return verification, nil
}

func (b CustodyBatch) containerTotals() (map[string]int, bool) {
	totals := map[string]int{string(ConditionNormal): 0, string(ConditionException): 0}
	complete := true
	for _, item := range b.Containers {
		if item.Condition != ConditionNormal && item.Condition != ConditionException {
			complete = false
			continue
		}
		totals[string(item.Condition)]++
	}
	return totals, complete
}

func (b *CustodyBatch) Receive(input ReceiveInput, now time.Time) error {
	if b.Status != StatusInTransit {
		return fmt.Errorf("%w: only IN_TRANSIT batches can be received", ErrInvalidTransition)
	}
	if len(input.Results) != len(b.Containers) {
		return fmt.Errorf("%w: receive results must match the manifest", ErrInvalidInput)
	}
	byID := make(map[string]ReceiveResult, len(input.Results))
	for _, result := range input.Results {
		id := strings.TrimSpace(result.ContainerID)
		if err := validateText(id, "received container id"); err != nil {
			return err
		}
		if !finite(result.ReceivedTemperatureC) {
			return fmt.Errorf("%w: received temperature must be finite", ErrInvalidInput)
		}
		key := manifestKey(id)
		if _, exists := byID[key]; exists {
			return fmt.Errorf("%w: duplicate received container id", ErrInvalidInput)
		}
		byID[key] = result
	}
	updatedContainers := b.Containers
	for index, item := range b.Containers {
		result, ok := byID[manifestKey(item.ContainerID)]
		if !ok {
			return fmt.Errorf("%w: missing received container %s", ErrInvalidInput, item.ContainerID)
		}
		itemCopy, err := evaluatedReceipt(item, result)
		if err != nil {
			return err
		}
		itemCopy.ReceiptStaged = false
		updatedContainers[index] = itemCopy
	}
	now = now.UTC()
	if err := b.validateLifecycleAppend(LifecycleReceived, b.Version+1, now); err != nil {
		return err
	}
	b.Containers = updatedContainers
	b.Status = StatusReceived
	b.Version++
	b.UpdatedAt = now
	b.recordLifecycleEvent(LifecycleReceived, now)
	b.RefreshReceiptProgress()
	return nil
}

func (b *CustodyBatch) StageReceipt(containerID string, result ReceiveResult, now time.Time) error {
	if b.Status != StatusInTransit {
		return fmt.Errorf("%w: only IN_TRANSIT batches can stage receipt results", ErrInvalidTransition)
	}
	id := strings.TrimSpace(containerID)
	if err := validateText(id, "received container id"); err != nil {
		return err
	}
	index := -1
	for currentIndex, item := range b.Containers {
		if strings.EqualFold(item.ContainerID, id) {
			index = currentIndex
			break
		}
	}
	if index < 0 {
		return fmt.Errorf("%w: unknown received container %s", ErrInvalidInput, id)
	}
	if b.Containers[index].ReceiptStaged {
		return fmt.Errorf("%w: receipt result already submitted for container %s", ErrInvalidInput, id)
	}
	item, err := evaluatedReceipt(b.Containers[index], result)
	if err != nil {
		return err
	}
	allSubmitted := true
	for currentIndex, current := range b.Containers {
		if currentIndex != index && !current.ReceiptStaged {
			allSubmitted = false
			break
		}
	}
	now = now.UTC()
	if allSubmitted {
		if err := b.validateLifecycleAppend(LifecycleReceived, b.Version+1, now); err != nil {
			return err
		}
	}
	item.ReceiptStaged = true
	updatedContainers := append([]Container(nil), b.Containers...)
	updatedContainers[index] = item
	b.Containers = updatedContainers
	if allSubmitted {
		for index := range b.Containers {
			b.Containers[index].ReceiptStaged = false
		}
		b.Status = StatusReceived
	}
	b.Version++
	b.UpdatedAt = now
	if allSubmitted {
		b.recordLifecycleEvent(LifecycleReceived, now)
	}
	b.RefreshReceiptProgress()
	return nil
}

func evaluatedReceipt(item Container, result ReceiveResult) (Container, error) {
	if !finite(result.ReceivedTemperatureC) {
		return Container{}, fmt.Errorf("%w: received temperature must be finite", ErrInvalidInput)
	}
	seal := strings.TrimSpace(result.ReceivedSealNumber)
	if seal == "" {
		return Container{}, fmt.Errorf("%w: received seal is required", ErrInvalidInput)
	}
	item.ReceivedSealNumber = seal
	item.ReceivedTemperatureC = result.ReceivedTemperatureC
	item.ExceptionNote = strings.TrimSpace(result.ExceptionNote)
	if seal == item.SealNumber && temperatureWithinRange(result.ReceivedTemperatureC, item.TemperatureMinC, item.TemperatureMaxC) {
		item.Condition = ConditionNormal
		item.ExceptionNote = ""
	} else {
		item.Condition = ConditionException
		if item.ExceptionNote == "" {
			item.ExceptionNote = "封签或温度核验异常"
		}
	}
	return item, nil
}

func (b *CustodyBatch) Close(receiptID string, now time.Time) error {
	if b.Status != StatusReceived {
		return fmt.Errorf("%w: only RECEIVED batches can be closed", ErrInvalidTransition)
	}
	if b.Receipt != nil {
		return fmt.Errorf("%w: batch is already closed", ErrInvalidTransition)
	}
	if err := validateText(receiptID, "receipt id"); err != nil {
		return err
	}
	totals := map[string]int{string(ConditionNormal): 0, string(ConditionException): 0}
	for _, item := range b.Containers {
		if item.Condition != ConditionNormal && item.Condition != ConditionException {
			return fmt.Errorf("%w: every container must be received", ErrInvalidInput)
		}
		totals[string(item.Condition)]++
	}
	now = now.UTC()
	if err := b.validateLifecycleAppend(LifecycleClosed, b.Version+1, now); err != nil {
		return err
	}
	b.Version++
	b.Status = StatusClosed
	b.UpdatedAt = now
	b.recordLifecycleEvent(LifecycleClosed, now)
	b.RefreshReceiptProgress()
	digest := b.DigestMaterial()
	b.Receipt = &CustodyReceipt{
		ReceiptID: strings.TrimSpace(receiptID), BatchID: b.ID, ClosedAt: now, ContainerTotals: totals,
		HandoffCount: len(b.Handoffs), FinalVersion: b.Version, Digest: digest,
	}
	return nil
}

func (b CustodyBatch) DigestMaterial() string {
	copyBatch := cloneBatch(b)
	copyBatch.Receipt = nil
	data, _ := json.Marshal(struct {
		ID                string           `json:"id"`
		Destination       string           `json:"destination"`
		ResponsiblePerson string           `json:"responsiblePerson"`
		Status            BatchStatus      `json:"status"`
		Containers        []Container      `json:"containers"`
		Handoffs          []HandoffEvent   `json:"handoffs"`
		LifecycleEvents   []LifecycleEvent `json:"lifecycleEvents"`
		Version           uint64           `json:"version"`
		CreatedAt         time.Time        `json:"createdAt"`
		UpdatedAt         time.Time        `json:"updatedAt"`
	}{copyBatch.ID, copyBatch.Destination, copyBatch.ResponsiblePerson, copyBatch.Status, copyBatch.Containers, copyBatch.Handoffs, copyBatch.LifecycleEvents, copyBatch.Version, copyBatch.CreatedAt, copyBatch.UpdatedAt})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (b CustodyBatch) ValidatePersisted() error {
	if err := validateText(b.ID, "batch id"); err != nil || b.Status == "" || b.Version == 0 || b.CreatedAt.IsZero() || b.UpdatedAt.IsZero() {
		return fmt.Errorf("%w: invalid batch envelope", ErrInvalidState)
	}
	if !validStatus(b.Status) {
		return fmt.Errorf("%w: unknown batch status", ErrInvalidState)
	}
	if err := b.validatePersistedLifecycleEvents(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	if len(b.Containers) == 0 {
		return fmt.Errorf("%w: batch has no containers", ErrInvalidState)
	}
	ids := map[string]bool{}
	seals := map[string]bool{}
	for _, item := range b.Containers {
		if strings.TrimSpace(item.ContainerID) == "" || strings.TrimSpace(item.SealNumber) == "" || !finite(item.TemperatureMinC) || !finite(item.TemperatureMaxC) || item.TemperatureMinC > item.TemperatureMaxC || ids[manifestKey(item.ContainerID)] || seals[manifestKey(item.SealNumber)] {
			return fmt.Errorf("%w: invalid persisted container", ErrInvalidState)
		}
		if item.Condition != ConditionPending && item.Condition != ConditionNormal && item.Condition != ConditionException {
			return fmt.Errorf("%w: invalid persisted condition", ErrInvalidState)
		}
		if item.ReceiptStaged && item.Condition != ConditionNormal && item.Condition != ConditionException {
			return fmt.Errorf("%w: staged receipt has no condition", ErrInvalidState)
		}
		if item.Condition == ConditionPending && (item.ReceivedSealNumber != "" || item.ExceptionNote != "" || item.ReceiptStaged) {
			return fmt.Errorf("%w: pending container has receipt data", ErrInvalidState)
		}
		if item.Condition == ConditionNormal || item.Condition == ConditionException {
			if strings.TrimSpace(item.ReceivedSealNumber) == "" || !finite(item.ReceivedTemperatureC) {
				return fmt.Errorf("%w: received container is incomplete", ErrInvalidState)
			}
			if item.Condition == ConditionNormal && (item.ReceivedSealNumber != item.SealNumber || item.ReceivedTemperatureC < item.TemperatureMinC || item.ReceivedTemperatureC > item.TemperatureMaxC || item.ExceptionNote != "") {
				return fmt.Errorf("%w: normal receipt does not match manifest", ErrInvalidState)
			}
			if item.Condition == ConditionException && strings.TrimSpace(item.ExceptionNote) == "" {
				return fmt.Errorf("%w: exception receipt needs a note", ErrInvalidState)
			}
		}
		ids[manifestKey(item.ContainerID)] = true
		seals[manifestKey(item.SealNumber)] = true
	}
	seenEventIDs := map[string]bool{}
	for index, event := range b.Handoffs {
		if event.Sequence != uint64(index+1) || strings.TrimSpace(event.IdempotencyKey) == "" || strings.TrimSpace(event.EventID) == "" || event.OccurredAt.IsZero() || event.RecordedAt.IsZero() {
			return fmt.Errorf("%w: invalid persisted handoff", ErrInvalidState)
		}
		if !validHandoffText(event) || seenEventIDs[normalizePerson(event.EventID)] {
			return fmt.Errorf("%w: invalid persisted handoff", ErrInvalidState)
		}
		if index > 0 {
			previous := b.Handoffs[index-1]
			if normalizePerson(event.FromPerson) != normalizePerson(previous.ToPerson) || event.OccurredAt.Before(previous.OccurredAt) {
				return fmt.Errorf("%w: invalid persisted handoff chain", ErrInvalidState)
			}
		}
		seenEventIDs[normalizePerson(event.EventID)] = true
	}
	if b.Status == StatusDraft && len(b.Handoffs) != 0 || b.Status == StatusDraft && b.Receipt != nil {
		return fmt.Errorf("%w: draft contains later state", ErrInvalidState)
	}
	if b.Status == StatusDraft {
		for _, item := range b.Containers {
			if item.Condition != ConditionPending || item.ReceivedSealNumber != "" || item.ExceptionNote != "" || item.ReceiptStaged {
				return fmt.Errorf("%w: transit batch contains receipt data", ErrInvalidState)
			}
		}
		if b.Receipt != nil {
			return fmt.Errorf("%w: open batch contains receipt", ErrInvalidState)
		}
	}
	if b.Status == StatusInTransit {
		for _, item := range b.Containers {
			if !item.ReceiptStaged && item.Condition != ConditionPending {
				return fmt.Errorf("%w: transit batch contains unapplied receipt", ErrInvalidState)
			}
		}
		if b.Receipt != nil {
			return fmt.Errorf("%w: open batch contains receipt", ErrInvalidState)
		}
	}
	if b.Status == StatusReceived {
		for _, item := range b.Containers {
			if (item.Condition != ConditionNormal && item.Condition != ConditionException) || item.ReceiptStaged {
				return fmt.Errorf("%w: received batch has pending container", ErrInvalidState)
			}
		}
		if b.Receipt != nil {
			return fmt.Errorf("%w: received batch contains receipt", ErrInvalidState)
		}
	}
	if b.Status == StatusClosed {
		for _, item := range b.Containers {
			if item.ReceiptStaged {
				return fmt.Errorf("%w: closed batch has staged receipt", ErrInvalidState)
			}
		}
		if b.Receipt == nil || b.Receipt.BatchID != b.ID || b.Receipt.FinalVersion != b.Version || b.Receipt.Digest == "" || b.Receipt.Digest != b.DigestMaterial() {
			return fmt.Errorf("%w: closed batch has invalid receipt", ErrInvalidState)
		}
	}
	if b.ReceiptProgress != nil && !equalProgress(*b.ReceiptProgress, b.Progress()) {
		return fmt.Errorf("%w: receipt progress does not match containers", ErrInvalidState)
	}
	return nil
}

func (b CustodyBatch) Clone() CustodyBatch { return cloneBatch(b) }

func (b CustodyBatch) HandoffByKey(key string) (HandoffEvent, bool) {
	for _, event := range b.Handoffs {
		if event.IdempotencyKey == key {
			return event, true
		}
	}
	return HandoffEvent{}, false
}

func HandoffMatches(event HandoffEvent, input HandoffInput) bool {
	return event.EventID == strings.TrimSpace(input.EventID) && event.Sequence == input.Sequence && event.FromPerson == strings.TrimSpace(input.FromPerson) && event.ToPerson == strings.TrimSpace(input.ToPerson) && event.Location == strings.TrimSpace(input.Location) && event.OccurredAt.Equal(input.OccurredAt.UTC())
}

func validateHandoffInput(input HandoffInput) error {
	for value, label := range map[string]string{input.EventID: "event id", input.IdempotencyKey: "idempotency key", input.FromPerson: "from person", input.ToPerson: "to person", input.Location: "location"} {
		if err := validateText(value, label); err != nil {
			return err
		}
	}
	if input.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred at is required", ErrInvalidInput)
	}
	return nil
}

func validStatus(status BatchStatus) bool {
	switch status {
	case StatusDraft, StatusInTransit, StatusReceived, StatusClosed:
		return true
	default:
		return false
	}
}

func validLifecycleEventType(eventType LifecycleEventType) bool {
	switch eventType {
	case LifecycleCreated, LifecycleManifestUpdated, LifecycleDispatched, LifecycleReceived, LifecycleClosed:
		return true
	default:
		return false
	}
}

func (b CustodyBatch) validateLifecycleAppend(eventType LifecycleEventType, version uint64, occurredAt time.Time) error {
	if !validLifecycleEventType(eventType) || version == 0 || occurredAt.IsZero() {
		return fmt.Errorf("生命周期事件无效")
	}
	occurredAt = occurredAt.UTC()
	if len(b.LifecycleEvents) == 0 {
		if eventType != LifecycleCreated || version != 1 {
			return fmt.Errorf("生命周期事件必须从 CREATED 开始")
		}
		return nil
	}
	previous := b.LifecycleEvents[len(b.LifecycleEvents)-1]
	if version <= previous.Version {
		return fmt.Errorf("生命周期事件版本必须递增")
	}
	switch previous.Type {
	case LifecycleCreated, LifecycleManifestUpdated:
		if eventType != LifecycleManifestUpdated && eventType != LifecycleDispatched {
			return fmt.Errorf("生命周期事件顺序无效")
		}
	case LifecycleDispatched:
		if eventType != LifecycleReceived {
			return fmt.Errorf("生命周期事件顺序无效")
		}
	case LifecycleReceived:
		if eventType != LifecycleClosed {
			return fmt.Errorf("生命周期事件顺序无效")
		}
	case LifecycleClosed:
		return fmt.Errorf("关闭后的批次不能追加生命周期事件")
	default:
		return fmt.Errorf("生命周期事件类型无效")
	}
	return nil
}

func (b *CustodyBatch) recordLifecycleEvent(eventType LifecycleEventType, occurredAt time.Time) {
	b.LifecycleEvents = append(b.LifecycleEvents, LifecycleEvent{
		Sequence: uint64(len(b.LifecycleEvents) + 1), Type: eventType, Version: b.Version, OccurredAt: occurredAt.UTC(),
	})
}

func (b CustodyBatch) validatePersistedLifecycleEvents() error {
	if len(b.LifecycleEvents) == 0 {
		return fmt.Errorf("缺少生命周期事件")
	}
	for index, event := range b.LifecycleEvents {
		if event.Sequence != uint64(index+1) || !validLifecycleEventType(event.Type) || event.Version == 0 || event.Version > b.Version || event.OccurredAt.IsZero() || event.OccurredAt.Location() != time.UTC {
			return fmt.Errorf("生命周期事件 %d 无效", index+1)
		}
		if index == 0 {
			if event.Type != LifecycleCreated || event.Version != 1 {
				return fmt.Errorf("首条生命周期事件无效")
			}
			continue
		}
		previous := b.LifecycleEvents[index-1]
		if event.Version <= previous.Version {
			return fmt.Errorf("生命周期事件版本不连续")
		}
		switch previous.Type {
		case LifecycleCreated, LifecycleManifestUpdated:
			if event.Type != LifecycleManifestUpdated && event.Type != LifecycleDispatched {
				return fmt.Errorf("生命周期事件顺序无效")
			}
		case LifecycleDispatched:
			if event.Type != LifecycleReceived {
				return fmt.Errorf("生命周期事件顺序无效")
			}
		case LifecycleReceived:
			if event.Type != LifecycleClosed {
				return fmt.Errorf("生命周期事件顺序无效")
			}
		case LifecycleClosed:
			return fmt.Errorf("关闭事件后仍有生命周期事件")
		default:
			return fmt.Errorf("生命周期事件类型无效")
		}
	}
	last := b.LifecycleEvents[len(b.LifecycleEvents)-1]
	switch b.Status {
	case StatusDraft:
		if (last.Type != LifecycleCreated && last.Type != LifecycleManifestUpdated) || last.Version != b.Version {
			return fmt.Errorf("DRAFT 状态与生命周期事件不一致")
		}
	case StatusInTransit:
		if last.Type != LifecycleDispatched {
			return fmt.Errorf("IN_TRANSIT 状态与生命周期事件不一致")
		}
	case StatusReceived:
		if last.Type != LifecycleReceived || last.Version != b.Version {
			return fmt.Errorf("RECEIVED 状态与生命周期事件不一致")
		}
	case StatusClosed:
		if last.Type != LifecycleClosed || last.Version != b.Version {
			return fmt.Errorf("CLOSED 状态与生命周期事件不一致")
		}
	}
	return nil
}

func validCondition(condition ReceiptCondition) bool {
	switch condition {
	case ConditionPending, ConditionNormal, ConditionException:
		return true
	default:
		return false
	}
}

func validHandoffText(event HandoffEvent) bool {
	for _, value := range []string{event.EventID, event.IdempotencyKey, event.FromPerson, event.ToPerson, event.Location} {
		if strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') {
			return false
		}
	}
	return true
}

func normalizePerson(value string) string { return foldCase(strings.TrimSpace(value)) }

func manifestKey(value string) string {
	return foldCase(strings.TrimSpace(value))
}

// foldCase normalizes a string using Unicode case folding so that case-equivalent
// runes share a single canonical key. Unlike strings.ToLower, this also treats
// non-ASCII equivalents such as U+017F (ſ) and U+0073 (s), or Greek final/regular
// sigma variants, as duplicates.
func foldCase(value string) string {
	if value == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(value))
	for _, r := range value {
		b.WriteRune(foldRune(r))
	}
	return b.String()
}

// foldRune returns the smallest rune in the Unicode SimpleFold cycle for r, so
// every rune that is case-equivalent under strings.EqualFold maps to the same
// representative rune.
func foldRune(r rune) rune {
	min := r
	for current := unicode.SimpleFold(r); current != r; current = unicode.SimpleFold(current) {
		if current < min {
			min = current
		}
	}
	return min
}

func temperatureWithinRange(value, minimum, maximum float64) bool {
	if !finite(value) || !finite(minimum) || !finite(maximum) || minimum > maximum {
		return false
	}
	if minimum == maximum {
		return false
	}
	return value >= minimum && value <= maximum
}

func normalizeRecordedAt(value time.Time) time.Time {
	if value.IsZero() {
		return value
	}
	return value
}

func equalTotals(left, right map[string]int) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func validateText(value, label string) error {
	if strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%w: %s is required", ErrInvalidInput, label)
	}
	return nil
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func cloneBatch(b CustodyBatch) CustodyBatch {
	copyBatch := b
	copyBatch.Containers = append([]Container(nil), b.Containers...)
	copyBatch.Handoffs = append([]HandoffEvent(nil), b.Handoffs...)
	copyBatch.LifecycleEvents = append([]LifecycleEvent(nil), b.LifecycleEvents...)
	if b.ReceiptProgress != nil {
		progress := *b.ReceiptProgress
		progress.PendingContainerIDs = append([]string(nil), b.ReceiptProgress.PendingContainerIDs...)
		copyBatch.ReceiptProgress = &progress
	}
	if b.Receipt != nil {
		receipt := *b.Receipt
		receipt.ContainerTotals = map[string]int{}
		for key, value := range b.Receipt.ContainerTotals {
			receipt.ContainerTotals[key] = value
		}
		copyBatch.Receipt = &receipt
	}
	return copyBatch
}

func equalProgress(left, right ReceiptProgress) bool {
	if left.SubmittedCount != right.SubmittedCount || left.TotalCount != right.TotalCount || len(left.PendingContainerIDs) != len(right.PendingContainerIDs) {
		return false
	}
	for index := range left.PendingContainerIDs {
		if left.PendingContainerIDs[index] != right.PendingContainerIDs[index] {
			return false
		}
	}
	return true
}

func SortedContainerIDs(b CustodyBatch) []string {
	ids := make([]string, 0, len(b.Containers))
	for _, item := range b.Containers {
		ids = append(ids, item.ContainerID)
	}
	sort.Strings(ids)
	return ids
}
