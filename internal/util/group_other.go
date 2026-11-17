//go:build !windows

package util

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
)

// Group is a started process together with every process it starts, so they
// can be stopped as one: a service started through a script or a launcher is
// stopped with its children. They share a process group.
type Group struct {
	cmd *exec.Cmd
}

// StartGroup starts cmd in a process group of its own.
func StartGroup(cmd *exec.Cmd) (*Group, error) {
	OwnProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &Group{cmd: cmd}, nil
}

// Terminate asks every process of the group to exit (SIGTERM).
func (g *Group) Terminate() error {
	return syscall.Kill(-g.cmd.Process.Pid, syscall.SIGTERM)
}

// Kill ends every process of the group (SIGKILL).
func (g *Group) Kill() error {
	return syscall.Kill(-g.cmd.Process.Pid, syscall.SIGKILL)
}

// Release frees what the group holds; nothing here.
func (g *Group) Release() {}

// ProcessImage returns the executable path of a running process. It needs
// /proc (Linux).
func ProcessImage(pid int) (string, error) {
	if _, err := os.Stat("/proc/self/exe"); err != nil {
		return "", errors.New("no /proc to identify processes")
	}
	return os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe")
}

// KillGroup ends a process left behind by an earlier run, and its process
// group, if it still runs image.
func KillGroup(pid int, image string) (bool, error) {
	got, err := ProcessImage(pid)
	if err != nil || !SamePath(got, image) {
		return false, nil // gone, or the pid now belongs to something else
	}
	return true, syscall.Kill(-pid, syscall.SIGKILL)
}

// SamePath compares two executable paths.
func SamePath(a, b string) bool { return filepath.Clean(a) == filepath.Clean(b) }
