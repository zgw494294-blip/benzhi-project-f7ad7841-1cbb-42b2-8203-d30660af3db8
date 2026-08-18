package ledger

import "errors"

var (
	ErrNotFound          = errors.New("batch not found")
	ErrAlreadyExists     = errors.New("batch already exists")
	ErrVersionConflict   = errors.New("ledger version conflict")
	ErrCorruptLedger     = errors.New("ledger file is corrupt")
	ErrUnsupportedFormat = errors.New("ledger format is unsupported")
)
