//go:build unix

package storage

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// ErrLocked means another process holds the run lock on this database.
var ErrLocked = errors.New("another pulse-github process holds the database lock")

// Lock is an exclusive advisory lock that keeps a single active process per database.
type Lock struct{ f *os.File }

// AcquireLock takes an exclusive, non-blocking flock on <dbPath>.lock. flock is reliable on local filesystems only;
// do not place the state volume on NFS or other network filesystems.
func AcquireLock(dbPath string) (*Lock, error) {
	f, err := os.OpenFile(dbPath+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("lock: %w", err)
	}
	return &Lock{f: f}, nil
}

// Release drops the lock.
func (l *Lock) Release() error {
	_ = unix.Flock(int(l.f.Fd()), unix.LOCK_UN)
	return l.f.Close()
}

// FreeBytes returns the bytes available to unprivileged users on the filesystem holding path.
func FreeBytes(path string) (uint64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil //nolint:gosec,unconvert // field widths differ per platform
}
