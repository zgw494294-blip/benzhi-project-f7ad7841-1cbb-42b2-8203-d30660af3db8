package custody

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"samplechain/internal/domain"
	"samplechain/internal/ledger"
)

type Ledger interface {
	Get(context.Context, string) (domain.CustodyBatch, error)
	List(context.Context) ([]domain.CustodyBatch, error)
	Create(context.Context, domain.CustodyBatch) error
	Commit(context.Context, string, uint64, domain.CustodyBatch) error
	Close() error
}

type Clock func() time.Time

type Service struct {
	ledger Ledger
	now    Clock
	ids    func(string) string
}

func NewService(store Ledger) (*Service, error) {
	if store == nil {
		return nil, errors.New("ledger is required")
	}
	return &Service{ledger: store, now: time.Now, ids: newIdentifier}, nil
}

func (s *Service) CreateBatch(ctx context.Context, input domain.NewBatchInput, expectedVersion uint64) (domain.CustodyBatch, error) {
	if expectedVersion != 0 {
		return domain.CustodyBatch{}, ErrVersionConflict
	}
	if strings.TrimSpace(input.ID) == "" {
		input.ID = s.ids("batch")
	}
	now := s.now().UTC()
	batch, err := domain.NewBatch(input, now)
	if err != nil {
		return domain.CustodyBatch{}, err
	}
	if err := contextErr(ctx); err != nil {
		return domain.CustodyBatch{}, err
	}
	if err := s.ledger.Create(ctx, batch); err != nil {
		return domain.CustodyBatch{}, err
	}
	return batch.Clone(), nil
}

func (s *Service) Dispatch(ctx context.Context, id string, expectedVersion uint64) (domain.CustodyBatch, error) {
	return s.update(ctx, id, expectedVersion, func(batch *domain.CustodyBatch) error {
		return batch.Dispatch(s.now())
	})
}

func (s *Service) UpdateManifest(ctx context.Context, id string, expectedVersion uint64, input domain.ManifestInput) (domain.CustodyBatch, error) {
	return s.update(ctx, id, expectedVersion, func(batch *domain.CustodyBatch) error {
		return batch.ReplaceDraftManifest(input, s.now())
	})
}

func (s *Service) AddHandoff(ctx context.Context, id string, expectedVersion uint64, input domain.HandoffInput) (domain.CustodyBatch, error) {
	current, err := s.ledger.Get(ctx, id)
	if err != nil {
		return domain.CustodyBatch{}, translateLedgerError(err)
	}
	if existing, found := current.HandoffByKey(strings.TrimSpace(input.IdempotencyKey)); found {
		if domain.HandoffMatches(existing, input) {
			return current, nil
		}
		return domain.CustodyBatch{}, ErrIdempotencyConflict
	}
	return s.updateLoaded(ctx, current, expectedVersion, func(batch *domain.CustodyBatch) error {
		_, err := batch.AddHandoff(input, s.now())
		return err
	})
}

func (s *Service) Receive(ctx context.Context, id string, expectedVersion uint64, input domain.ReceiveInput) (domain.CustodyBatch, error) {
	return s.update(ctx, id, expectedVersion, func(batch *domain.CustodyBatch) error {
		return batch.Receive(input, s.now())
	})
}

func (s *Service) StageReceipt(ctx context.Context, id string, containerID string, expectedVersion uint64, input domain.ReceiveResult) (domain.CustodyBatch, error) {
	return s.update(ctx, id, expectedVersion, func(batch *domain.CustodyBatch) error {
		return batch.StageReceipt(containerID, input, s.now())
	})
}

func (s *Service) CloseBatch(ctx context.Context, id string, expectedVersion uint64) (domain.CustodyBatch, error) {
	return s.update(ctx, id, expectedVersion, func(batch *domain.CustodyBatch) error {
		return batch.Close(s.ids("receipt"), s.now())
	})
}

func (s *Service) GetBatch(ctx context.Context, id string) (domain.CustodyBatch, error) {
	batch, err := s.ledger.Get(ctx, id)
	if err != nil {
		return domain.CustodyBatch{}, translateLedgerError(err)
	}
	ensureBatchProgress(&batch)
	return batch, nil
}

func (s *Service) ListBatches(ctx context.Context, query domain.BatchListQuery) (domain.BatchListResult, error) {
	if err := query.Validate(); err != nil {
		return domain.BatchListResult{}, err
	}
	batches, err := s.ledger.List(ctx)
	if err != nil {
		return domain.BatchListResult{}, err
	}
	filtered := make([]domain.CustodyBatch, 0, len(batches))
	totals := domain.NewBatchListTotals()
	for _, batch := range batches {
		if err := checkListContext(ctx); err != nil {
			return domain.BatchListResult{}, err
		}
		batch.EnsureReceiptProgress()
		if batch.MatchesQuery(query) {
			totals.Add(batch)
			filtered = append(filtered, batch)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].UpdatedAt.Equal(filtered[j].UpdatedAt) {
			return filtered[i].ID < filtered[j].ID
		}
		return filtered[i].UpdatedAt.Before(filtered[j].UpdatedAt)
	})
	start := 0
	if query.Cursor != "" {
		cursor, err := decodeBatchCursor(query.Cursor)
		if err != nil {
			return domain.BatchListResult{}, err
		}
		start = len(filtered)
		for index, batch := range filtered {
			if afterCursor(batch, cursor) {
				start = index
				break
			}
		}
	}
	end := start + query.Limit
	if end > len(filtered) {
		end = len(filtered)
	}
	result := domain.BatchListResult{Items: make([]domain.BatchSummary, 0, end-start), Totals: totals}
	for _, batch := range filtered[start:end] {
		result.Items = append(result.Items, batch.Summary())
	}
	if end < len(filtered) && end > start {
		last := filtered[end-1]
		result.NextCursor = encodeBatchCursor(batchCursor{UpdatedAt: last.UpdatedAt, ID: last.ID})
	}
	return result, nil
}

func (s *Service) VerifyReceipt(ctx context.Context, id string) (domain.ReceiptVerification, error) {
	batch, err := s.GetBatch(ctx, id)
	if err != nil {
		return domain.ReceiptVerification{}, err
	}
	return batch.VerifyReceipt()
}

func (s *Service) Close() error { return s.ledger.Close() }

func (s *Service) update(ctx context.Context, id string, expectedVersion uint64, mutate func(*domain.CustodyBatch) error) (domain.CustodyBatch, error) {
	current, err := s.ledger.Get(ctx, id)
	if err != nil {
		return domain.CustodyBatch{}, translateLedgerError(err)
	}
	return s.updateLoaded(ctx, current, expectedVersion, mutate)
}

func (s *Service) updateLoaded(ctx context.Context, current domain.CustodyBatch, expectedVersion uint64, mutate func(*domain.CustodyBatch) error) (domain.CustodyBatch, error) {
	if current.Version != expectedVersion {
		return domain.CustodyBatch{}, ErrVersionConflict
	}
	if err := contextErr(ctx); err != nil {
		return domain.CustodyBatch{}, err
	}
	candidate := current.Clone()
	if err := mutate(&candidate); err != nil {
		return domain.CustodyBatch{}, err
	}
	if err := contextErr(ctx); err != nil {
		return domain.CustodyBatch{}, err
	}
	if err := s.ledger.Commit(ctx, candidate.ID, current.Version, candidate); err != nil {
		if errors.Is(err, ledger.ErrVersionConflict) {
			return domain.CustodyBatch{}, ErrVersionConflict
		}
		return domain.CustodyBatch{}, err
	}
	return candidate.Clone(), nil
}

func translateLedgerError(err error) error {
	if errors.Is(err, ledger.ErrNotFound) {
		return ledger.ErrNotFound
	}
	return err
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func newIdentifier(prefix string) string {
	buffer := make([]byte, 8)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(buffer)
}

type batchCursor struct {
	UpdatedAt time.Time `json:"updatedAt"`
	ID        string    `json:"id"`
}

func encodeBatchCursor(cursor batchCursor) string {
	data, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeBatchCursor(value string) (batchCursor, error) {
	if len(value) > 512 {
		return batchCursor{}, fmt.Errorf("%w: cursor is too long", domain.ErrInvalidQuery)
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return batchCursor{}, fmt.Errorf("%w: cursor is invalid", domain.ErrInvalidQuery)
	}
	var cursor batchCursor
	if err := json.Unmarshal(data, &cursor); err != nil || cursor.ID == "" || cursor.UpdatedAt.IsZero() {
		return batchCursor{}, fmt.Errorf("%w: cursor is invalid", domain.ErrInvalidQuery)
	}
	return cursor, nil
}

func afterCursor(batch domain.CustodyBatch, cursor batchCursor) bool {
	if batch.UpdatedAt.Equal(cursor.UpdatedAt) {
		return batch.ID > cursor.ID
	}
	return batch.UpdatedAt.After(cursor.UpdatedAt)
}

func checkListContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return nil
	default:
		return nil
	}
}

func ensureBatchProgress(batch *domain.CustodyBatch) {
	if batch == nil {
		return
	}
	expected := batch.Progress()
	if receiptProgressMatches(batch.ReceiptProgress, expected) {
		return
	}
	batch.ReceiptProgress = &expected
}

func receiptProgressMatches(actual *domain.ReceiptProgress, expected domain.ReceiptProgress) bool {
	if actual == nil || actual.SubmittedCount != expected.SubmittedCount || actual.TotalCount != expected.TotalCount {
		return false
	}
	if len(actual.PendingContainerIDs) != len(expected.PendingContainerIDs) {
		return false
	}
	for index, containerID := range expected.PendingContainerIDs {
		if actual.PendingContainerIDs[index] != containerID {
			return false
		}
	}
	return true
}
