//go:build !windows && !linux

package timectl

import (
	"errors"
	"time"
)

func setClock(time.Time) error {
	return errors.New("setting the system clock is not implemented on this platform")
}
