// +build !openbsd

package migrate

// Pledge is only supported on OpenBSD.
func Pledge() error { return nil }

// Unveil is only supported on OpenBSD.
func Unveil(dir string) error { return nil }
