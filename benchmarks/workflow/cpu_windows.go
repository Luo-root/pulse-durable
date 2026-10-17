//go:build windows

package main

import (
	"syscall"
	"unsafe"
)

var counterProc = syscall.NewLazyDLL("kernel32.dll").NewProc("QueryPerformanceCounter")
var frequencyProc = syscall.NewLazyDLL("kernel32.dll").NewProc("QueryPerformanceFrequency")
var clockFrequency int64

// These APIs always succeed on Windows XP and later.
func init() { frequencyProc.Call(uintptr(unsafe.Pointer(&clockFrequency))) }
func clockNS() int64 {
	var counter int64
	counterProc.Call(uintptr(unsafe.Pointer(&counter)))
	return counter/clockFrequency*1e9 + counter%clockFrequency*1e9/clockFrequency
}

func processCPU() (int64, error) {
	h, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0, err
	}
	var created, exited, kernel, user syscall.Filetime
	if err := syscall.GetProcessTimes(h, &created, &exited, &kernel, &user); err != nil {
		return 0, err
	}
	ticks := uint64(kernel.HighDateTime)<<32 | uint64(kernel.LowDateTime)
	ticks += uint64(user.HighDateTime)<<32 | uint64(user.LowDateTime)
	return int64(ticks) * 100, nil
}
