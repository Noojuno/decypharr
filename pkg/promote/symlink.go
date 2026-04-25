package promote

import (
	"fmt"
	"os"
	"path/filepath"
)

// Repoint atomically replaces the symlink at linkPath so that it points at
// newTarget. POSIX rename(2) replaces an existing symlink in a single step,
// so any reader either sees the old target or the new one — never a missing
// path. Safe to call when linkPath does not yet exist (acts as a plain
// Symlink).
func Repoint(linkPath, newTarget string) error {
	if err := os.MkdirAll(filepath.Dir(linkPath), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(linkPath), err)
	}

	tmp := linkPath + ".repoint.tmp"
	// Clear any stale tmp from a prior aborted call so os.Symlink doesn't EEXIST.
	_ = os.Remove(tmp)

	if err := os.Symlink(newTarget, tmp); err != nil {
		return fmt.Errorf("symlink %s -> %s: %w", tmp, newTarget, err)
	}

	if err := os.Rename(tmp, linkPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s -> %s: %w", tmp, linkPath, err)
	}
	return nil
}

// ReadTarget returns the target of the symlink at linkPath. Wraps os.Readlink
// with an error wrap so callers don't need to repeat the path in their
// messages.
func ReadTarget(linkPath string) (string, error) {
	target, err := os.Readlink(linkPath)
	if err != nil {
		return "", fmt.Errorf("readlink %s: %w", linkPath, err)
	}
	return target, nil
}

// IsSymlink reports whether the path exists and is a symlink. Used as a
// sanity check before repointing — we never want to clobber a regular file
// the user (or another tool) put there.
func IsSymlink(linkPath string) (bool, error) {
	info, err := os.Lstat(linkPath)
	if err != nil {
		return false, err
	}
	return info.Mode()&os.ModeSymlink != 0, nil
}
