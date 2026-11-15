package timectl

import (
	"syscall"
	"time"
	"unsafe"
)

var procSetSystemTime = syscall.NewLazyDLL("kernel32.dll").NewProc("SetSystemTime")

// setClock sets the system clock. The process needs the privilege to change
// the system time (administrators have it).
func setClock(t time.Time) error {
	t = t.UTC()
	st := struct {
		Year, Month, DayOfWeek, Day, Hour, Minute, Second, Milliseconds uint16
	}{
		Year: uint16(t.Year()), Month: uint16(t.Month()), Day: uint16(t.Day()),
		Hour: uint16(t.Hour()), Minute: uint16(t.Minute()), Second: uint16(t.Second()),
		Milliseconds: uint16(t.Nanosecond() / 1e6),
	}
	if ok, _, err := procSetSystemTime.Call(uintptr(unsafe.Pointer(&st))); ok == 0 {
		return err
	}
	return nil
}
