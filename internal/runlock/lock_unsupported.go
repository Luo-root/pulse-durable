//go:build !windows && !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package runlock

import (
	"errors"
	"os"
)

func lockFile(*os.File) error   { return errors.ErrUnsupported }
func unlockFile(*os.File) error { return errors.ErrUnsupported }
