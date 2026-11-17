package util

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Group is a started process together with every process it starts, so they
// can be stopped as one: a service started through a .bat or a launcher is
// stopped with its children.
//
// On Windows the processes are in a job object that is killed when its last
// handle closes: if this program dies, the OS stops them too.
type Group struct {
	cmd *exec.Cmd
	job windows.Handle
}

// StartGroup starts cmd in a group of its own.
func StartGroup(cmd *exec.Cmd) (*Group, error) {
	OwnProcessGroup(cmd)
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("job object: %w", err)
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		windows.CloseHandle(job)
		return nil, fmt.Errorf("job object: %w", err)
	}
	if err := cmd.Start(); err != nil {
		windows.CloseHandle(job)
		return nil, err
	}
	// A child the process starts in the instant before this is not in the
	// job; Go cannot start a process suspended to close that gap.
	h, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err == nil {
		err = windows.AssignProcessToJobObject(job, h)
		windows.CloseHandle(h)
	}
	if err != nil {
		_ = cmd.Process.Kill()
		windows.CloseHandle(job)
		return nil, fmt.Errorf("job object: %w", err)
	}
	return &Group{cmd: cmd, job: job}, nil
}

// Terminate asks the group to exit. Windows has no such request for a
// process without a window, so it ends the group at once, like Kill.
func (g *Group) Terminate() error { return g.Kill() }

// Kill ends every process of the group.
func (g *Group) Kill() error {
	if g.job == 0 {
		return g.cmd.Process.Kill()
	}
	return windows.TerminateJobObject(g.job, 1)
}

// Release frees what the group holds, once its process has exited. Processes
// it started that still run are ended with it.
func (g *Group) Release() {
	if g.job != 0 {
		windows.CloseHandle(g.job)
		g.job = 0
	}
}

// ProcessImage returns the executable path of a running process.
func ProcessImage(pid int) (string, error) {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(h)
	buf := make([]uint16, windows.MAX_LONG_PATH)
	size := uint32(len(buf))
	if err := windows.QueryFullProcessImageName(h, 0, &buf[0], &size); err != nil {
		return "", err
	}
	return windows.UTF16ToString(buf[:size]), nil
}

// KillGroup ends a process left behind by an earlier run, and what it
// started, if it still runs image.
func KillGroup(pid int, image string) (bool, error) {
	got, err := ProcessImage(pid)
	if err != nil || !SamePath(got, image) {
		return false, nil // gone, or the pid now belongs to something else
	}
	// Its job object closed with the program that made it, so normally
	// nothing is left; this is for a process that escaped it.
	h, err := windows.OpenProcess(windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		return false, err
	}
	defer windows.CloseHandle(h)
	return true, windows.TerminateProcess(h, 1)
}

// SamePath compares two executable paths the way the file system does.
func SamePath(a, b string) bool {
	return strings.EqualFold(filepath.Clean(a), filepath.Clean(b))
}
