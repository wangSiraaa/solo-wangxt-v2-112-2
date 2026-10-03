//go:build linux

package backup

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// openExclusiveFile creates a new regular file that must not already exist.
// O_NOFOLLOW blocks the classic escape: if an attacker pre-creates a symlink
// at the restore path pointing outside the root, this open fails instead of
// writing through the link.
func openExclusiveFile(pathname string, mode os.FileMode) (*os.File, error) {
	fd, err := syscall.Open(pathname, syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, uint32(mode.Perm()))
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: pathname, Err: err}
	}
	return os.NewFile(uintptr(fd), pathname), nil
}

// renameNoReplace atomically publishes the verified staging tree at target.
// RENAME_NOREPLACE makes the no-overwrite guarantee atomic with the rename
// itself: if anything appeared at target since the precheck, the publish
// fails with ErrTargetExists instead of silently replacing it.
func renameNoReplace(staging, target string) error {
	err := unix.Renameat2(unix.AT_FDCWD, staging, unix.AT_FDCWD, target, unix.RENAME_NOREPLACE)
	if errors.Is(err, unix.EEXIST) || errors.Is(err, unix.ENOTEMPTY) {
		return ErrTargetExists
	}
	if err != nil {
		return &os.PathError{Op: "rename", Path: target, Err: err}
	}
	return nil
}

func chown(path string, uid, gid int) {
	_ = syscall.Chown(path, uid, gid) // best effort; non-root restores skip ownership
}

// lchown sets ownership of the link itself, never of its target.
func lchown(path string, uid, gid int) {
	_ = unix.Fchownat(unix.AT_FDCWD, path, uid, gid, unix.AT_SYMLINK_NOFOLLOW)
}
