//go:build !windows

package timectl

import (
	"errors"
	"time"
)

func setClock(time.Time) error {
	return errors.New("setting the system clock is only implemented for Windows")
}
