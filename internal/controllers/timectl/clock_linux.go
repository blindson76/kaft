package timectl

import (
	"syscall"
	"time"
)

// setClock sets the system clock. The process needs CAP_SYS_TIME.
func setClock(t time.Time) error {
	tv := syscall.NsecToTimeval(t.UnixNano())
	return syscall.Settimeofday(&tv)
}
