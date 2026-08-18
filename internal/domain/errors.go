package domain

import "errors"

var (
	ErrInvalidInput      = errors.New("invalid domain input")
	ErrInvalidTransition = errors.New("invalid batch transition")
	ErrInvalidState      = errors.New("invalid persisted state")
	ErrInvalidQuery      = errors.New("invalid batch query")
)
