//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package runlock

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	durable "github.com/Luo-root/pulse-durable"
)

func lockFile(file *os.File) error {
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return durable.ErrBusy
		}
		if err != nil {
			return fmt.Errorf("runlock: lock file: %w", err)
		}
		return nil
	}
}

func unlockFile(file *os.File) error {
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("runlock: unlock file: %w", err)
		}
		return nil
	}
}
