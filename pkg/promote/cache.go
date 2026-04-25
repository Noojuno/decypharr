// Package promote owns the "promote a streamed entry to a local file"
// architecture. It is split into three layers so each can evolve
// independently:
//
//   - Cache (this file): owns the local file directory. Knows nothing about
//     symlinks or *arrs. Pure filesystem operations on a stable path scheme.
//   - Symlink router (symlink.go): atomic repoint of an existing symlink.
//   - Orchestrator (orchestrator.go): drives the state machine for an entry —
//     download to cache, then repoint the *arr library symlink.
//
// Eviction (not implemented yet) would attach to the cache + router layers
// without touching the orchestrator's contract.
package promote

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Cache is the local file cache layer. The on-disk layout is
// <root>/<infohash>/<filename>, which is stable across *arr renames and
// trivial to reason about for orphan detection.
type Cache struct {
	root string
}

// NewCache creates a cache rooted at the given path. The directory is created
// lazily on first write.
func NewCache(root string) *Cache {
	return &Cache{root: root}
}

// Path returns the canonical local path for a given infohash + filename. The
// returned path may not exist; use Exists to check.
func (c *Cache) Path(infoHash, filename string) string {
	return filepath.Join(c.root, infoHash, filename)
}

// Exists reports whether a usable local copy is present.
func (c *Cache) Exists(infoHash, filename string) bool {
	st, err := os.Stat(c.Path(infoHash, filename))
	return err == nil && !st.IsDir()
}

// Write streams body into the cache via a .part temp file, then atomically
// renames it to the final path. The destination directory is created if
// needed. On any error the temp file is cleaned up.
func (c *Cache) Write(infoHash, filename string, body io.Reader) error {
	final := c.Path(infoHash, filename)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return fmt.Errorf("mkdir cache dir: %w", err)
	}

	tmp := final + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}

	if _, err := io.Copy(f, body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}

	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s -> %s: %w", tmp, final, err)
	}
	return nil
}

// Delete removes a cached file. Missing files are not an error.
func (c *Cache) Delete(infoHash, filename string) error {
	if err := os.Remove(c.Path(infoHash, filename)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
