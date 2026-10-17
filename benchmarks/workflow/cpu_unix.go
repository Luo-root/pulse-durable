//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package main

import (
	"syscall"
	"time"
)

var clockOrigin = time.Now()

func clockNS() int64 { return time.Since(clockOrigin).Nanoseconds() }

func processCPU() (int64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	return usage.Utime.Nano() + usage.Stime.Nano(), nil
}
