//go:build !windows

package taskctl

import "syscall"

// running reports whether a process with pid exists and has not exited.
func running(pid int) bool {
	var ws syscall.WaitStatus
	// A grandchild is not ours to wait for; signal 0 only checks it exists.
	_, _ = syscall.Wait4(pid, &ws, syscall.WNOHANG, nil)
	return syscall.Kill(pid, 0) == nil
}
