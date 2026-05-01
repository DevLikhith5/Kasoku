package validate

import (
	"fmt"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

func Key(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if len(key) > storage.MaxKeyLen {
		return fmt.Errorf("key too long (max %d bytes)", storage.MaxKeyLen)
	}
	return nil
}

func Value(value string) error {
	if len(value) > storage.MaxValueLen {
		return fmt.Errorf("value too long (max %d bytes)", storage.MaxValueLen)
	}
	return nil
}
