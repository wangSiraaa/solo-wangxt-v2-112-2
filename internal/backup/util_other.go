//go:build !linux

package backup

import (
	"errors"
	"os"
)

func openExclusiveFile(path string, mode os.FileMode) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, mode.Perm())
}

// renameNoReplace publishes staging at target. Platforms without renameat2
// fall back to a check-then-rename; the create-time precheck already rejected
// existing targets, so this only guards the narrow publish race.
func renameNoReplace(staging, target string) error {
	if _, err := os.Lstat(target); err == nil {
		return ErrTargetExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(staging, target)
}

func chown(path string, uid, gid int) {}

func lchown(path string, uid, gid int) {}
