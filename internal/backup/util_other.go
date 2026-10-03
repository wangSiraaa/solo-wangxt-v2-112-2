//go:build !linux

package backup

import (
	"fmt"
	"os"
)

func openExclusiveFile(path string, mode os.FileMode) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, mode.Perm())
}

func chown(path string, uid, gid int) {}

func lchown(path string, uid, gid int) {}

func renameNoReplace(old, new string) error {
	return fmt.Errorf("atomic no-replace restore publish is unsupported on this platform")
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
