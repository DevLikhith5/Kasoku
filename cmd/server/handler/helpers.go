package handler

import (
	"errors"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

// Error helpers

func isKeyNotFound(err error) bool {
	return errors.Is(err, storage.ErrKeyNotFound)
}

func isKeyTooLong(err error) bool {
	return errors.Is(err, storage.ErrKeyTooLong)
}

func isValueTooLarge(err error) bool {
	return errors.Is(err, storage.ErrValueTooLarge)
}
