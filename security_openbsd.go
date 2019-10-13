package migrate

import (
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// Pledge to the kernel the required syscalls on OpenBSD.
func Pledge() error {
	const promises = "stdio rpath inet"
	if err := unix.Pledge(promises, ""); err != nil {
		return err
	}
	return nil
}

// Unveil only specific directories containing migrations and/or TLS certs to
// the program.
func Unveil(paths []string) error {
	for _, p := range paths {
		if err := unix.Unveil(p, "r"); err != nil {
			return errors.Wrapf(err, "unveil %s", p)
		}
	}
	if err := unix.UnveilBlock(); err != nil {
		return errors.Wrap(err, "unveil block")
	}
	return nil
}
