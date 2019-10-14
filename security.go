// +build !openbsd

package migrate

// Pledge is only supported on OpenBSD.
func Pledge() error { return nil }

// Unveil is only supported on OpenBSD.
func Unveil(paths []string) error { return nil }
