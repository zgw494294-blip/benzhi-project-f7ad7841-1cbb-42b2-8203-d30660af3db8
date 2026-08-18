package custody

import "errors"

var (
	ErrVersionConflict     = errors.New("expected version does not match")
	ErrIdempotencyConflict = errors.New("idempotency key has different content")
)
