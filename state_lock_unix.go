//go:build linux || darwin

package main

import (
	"os"
	"syscall"
)

func lockState(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) }
func unlockState(f *os.File)     { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
