//go:build !linux && !darwin

package main

import (
	"fmt"
	"os"
)

func lockState(f *os.File) error { return fmt.Errorf("management requires Linux or macOS") }
func unlockState(f *os.File)     {}
