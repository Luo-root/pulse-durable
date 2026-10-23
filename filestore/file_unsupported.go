//go:build !windows && !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package filestore

import "errors"

func replaceFile(string, string) error {
	return errors.ErrUnsupported
}

func syncDirectory(string) error {
	return errors.ErrUnsupported
}
