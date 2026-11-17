package util

import (
	"os/exec"
	"syscall"
)

// OwnProcessGroup starts cmd in a process group of its own, so a Ctrl+C or
// Ctrl+Break in this program's console reaches only this program. The service
// is then stopped by it, in its shutdown order, instead of on its own at the
// same moment.
func OwnProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP
}
