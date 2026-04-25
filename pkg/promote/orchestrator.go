package promote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

// State is the lifecycle of an in-flight promotion. Held in memory for v1
// (not persisted) — schema-backed state can be added later without changing
// the orchestrator's contract.
type State string

const (
	StateIdle        State = ""
	StatePending     State = "pending"
	StateDownloading State = "downloading"
	StateRepointing  State = "repointing"
	StateComplete    State = "complete"
	StateFailed      State = "failed"
)

// Status is a snapshot of an entry's promotion progress. Returned by the
// orchestrator for UI/API consumption.
type Status struct {
	State    State
	Bytes    int64
	Total    int64
	Progress float64
	Error    string
}

// Resolver provides the orchestrator with the data + I/O it needs without
// coupling it to the Manager type. Manager implements this; tests can supply
// a fake.
type Resolver interface {
	// EntryFiles returns the (filename, size, downloadLink) tuples to backfill.
	// downloadLink should be a fully-resolved HTTP URL for the cached debrid file.
	EntryFiles(ctx context.Context, infoHash string) ([]EntryFile, error)
	// EntryCategory returns the *arr category the entry was imported under.
	EntryCategory(infoHash string) (string, error)
	// ArrFor returns the *arr handle for a category, or nil if none configured.
	ArrFor(category string) *arr.Arr
	// MountPath returns the decypharr FUSE mount root. Used to verify the
	// library symlink we're about to repoint actually points into our mount —
	// we never want to clobber an unrelated symlink.
	MountPath() string
	// HTTPClient returns the client used to fetch debrid download links.
	HTTPClient() *http.Client
	// PersistStatus writes the current backfill state to the entry's
	// persistent record so it survives restarts. May be called frequently
	// during a backfill; implementations should be cheap.
	PersistStatus(infoHash string, status Status)
	// SetFileLocalPath records that a file now has a local cache copy at the
	// given path. Empty path clears the record (used during demote/eviction).
	SetFileLocalPath(infoHash, fileName, localPath string)
}

// EntryFile is the per-file payload the orchestrator needs to backfill one
// item from a torrent.
type EntryFile struct {
	Name         string
	Size         int64
	DownloadLink string
}

// Orchestrator drives the per-entry promotion state machine. Concurrent
// promotions on the same entry are deduplicated; concurrent promotions on
// different entries run in parallel.
type Orchestrator struct {
	cache    *Cache
	resolver Resolver
	logger   zerolog.Logger

	mu     sync.Mutex
	states map[string]*Status // infohash -> live status
	locks  map[string]*sync.Mutex
}

// NewOrchestrator wires a cache + resolver into a runnable orchestrator.
func NewOrchestrator(cache *Cache, resolver Resolver, logger zerolog.Logger) *Orchestrator {
	return &Orchestrator{
		cache:    cache,
		resolver: resolver,
		logger:   logger.With().Str("component", "promote").Logger(),
		states:   make(map[string]*Status),
		locks:    make(map[string]*sync.Mutex),
	}
}

// Status returns a snapshot of the current promotion state for an entry, or
// the zero value if no promotion is in flight or recorded.
func (o *Orchestrator) Status(infoHash string) Status {
	o.mu.Lock()
	defer o.mu.Unlock()
	if s, ok := o.states[infoHash]; ok {
		return *s
	}
	return Status{}
}

// CachePath returns the path a cached file would have for the given entry +
// filename, regardless of whether that file currently exists. Used by the
// startup sweep to verify or repair library symlinks against the cache.
func (o *Orchestrator) CachePath(infoHash, fileName string) string {
	return o.cache.Path(infoHash, fileName)
}

// CacheHas reports whether a usable local copy is present.
func (o *Orchestrator) CacheHas(infoHash, fileName string) bool {
	return o.cache.Exists(infoHash, fileName)
}

// Sweep runs Promote in the background for each given infohash, bounded by
// the number of concurrent goroutines. Returns immediately. Used by the
// startup + periodic self-healing sweeps; safe to call repeatedly because
// Promote itself is idempotent (already-cached files skip download, already-
// repointed symlinks skip the swap).
func (o *Orchestrator) Sweep(ctx context.Context, infoHashes []string, concurrency int) {
	if concurrency < 1 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	for _, hash := range infoHashes {
		sem <- struct{}{}
		go func(h string) {
			defer func() { <-sem }()
			if err := o.Promote(ctx, h); err != nil {
				o.logger.Debug().Err(err).Str("infohash", h).Msg("Sweep promote: deferred")
			}
		}(hash)
	}
}

// Promote runs the full state machine for an entry: download each file into
// the local cache, then repoint the *arr library symlink at the cached file.
// Blocks until done. Per-entry mutex prevents concurrent promotions of the
// same entry; concurrent calls on different entries proceed in parallel.
func (o *Orchestrator) Promote(ctx context.Context, infoHash string) error {
	lock := o.lockFor(infoHash)
	lock.Lock()
	defer lock.Unlock()

	files, err := o.resolver.EntryFiles(ctx, infoHash)
	if err != nil {
		o.setStatus(infoHash, &Status{State: StateFailed, Error: err.Error()})
		return fmt.Errorf("resolve entry files: %w", err)
	}
	if len(files) == 0 {
		o.setStatus(infoHash, &Status{State: StateComplete})
		return nil
	}

	var total int64
	for _, f := range files {
		total += f.Size
	}
	status := &Status{State: StatePending, Total: total}
	o.setStatus(infoHash, status)

	category, err := o.resolver.EntryCategory(infoHash)
	if err != nil {
		o.fail(infoHash, status, fmt.Errorf("resolve category: %w", err))
		return err
	}
	bridge := arr.BridgeFor(o.resolver.ArrFor(category))
	if bridge == nil {
		err := fmt.Errorf("no library bridge for category %q (unsupported *arr type)", category)
		o.fail(infoHash, status, err)
		return err
	}

	status.State = StateDownloading
	o.setStatus(infoHash, status)

	for _, file := range files {
		if err := ctx.Err(); err != nil {
			o.fail(infoHash, status, err)
			return err
		}
		if !o.cache.Exists(infoHash, file.Name) {
			if err := o.downloadOne(ctx, infoHash, file, status); err != nil {
				o.fail(infoHash, status, fmt.Errorf("download %s: %w", file.Name, err))
				return err
			}
		} else {
			// Already cached — count its bytes toward progress and move on.
			status.Bytes += file.Size
			o.updateProgress(infoHash, status)
		}
		o.resolver.SetFileLocalPath(infoHash, file.Name, o.cache.Path(infoHash, file.Name))
	}

	status.State = StateRepointing
	o.setStatus(infoHash, status)

	mountPath := o.resolver.MountPath()
	for _, file := range files {
		libFile, err := bridge.FindLibraryFile(infoHash, file.Name)
		if errors.Is(err, arr.ErrLibraryFileNotFound) {
			o.logger.Warn().Str("file", file.Name).Msg("Promotion: *arr has no imported library file — local copy stays in cache, will retry on next sweep")
			continue
		}
		if err != nil {
			o.fail(infoHash, status, fmt.Errorf("find library file for %s: %w", file.Name, err))
			return err
		}
		if err := o.repointOne(libFile, mountPath, o.cache.Path(infoHash, file.Name)); err != nil {
			o.fail(infoHash, status, fmt.Errorf("repoint %s: %w", libFile.Path, err))
			return err
		}
		o.logger.Info().Str("library", libFile.Path).Str("local", o.cache.Path(infoHash, file.Name)).Msg("Promotion: library symlink now points at local copy")
	}

	status.State = StateComplete
	status.Progress = 1.0
	o.setStatus(infoHash, status)
	return nil
}

// downloadOne fetches a single file via HTTP and streams it into the cache.
// Progress is reported into the shared status snapshot.
func (o *Orchestrator) downloadOne(ctx context.Context, infoHash string, file EntryFile, status *Status) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, file.DownloadLink, nil)
	if err != nil {
		return err
	}
	resp, err := o.resolver.HTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	progress := &progressReader{
		r:        resp.Body,
		onTick:   func(n int64) { status.Bytes += n; o.updateProgress(infoHash, status) },
		interval: 1 << 20, // report every 1MB
	}
	return o.cache.Write(infoHash, file.Name, progress)
}

// repointOne validates that the library file is a symlink targeting our FUSE
// mount, then atomically rewrites it to the local cache path. If the library
// file is already pointing at the local path, it's a no-op. If it's a real
// file or points somewhere unexpected, we refuse to touch it.
func (o *Orchestrator) repointOne(libFile arr.LibraryFile, mountPath, localPath string) error {
	isLink, err := IsSymlink(libFile.Path)
	if err != nil {
		return fmt.Errorf("lstat %s: %w", libFile.Path, err)
	}
	if !isLink {
		return fmt.Errorf("library file %s is a regular file, not a symlink — refusing to overwrite", libFile.Path)
	}
	target, err := ReadTarget(libFile.Path)
	if err != nil {
		return err
	}
	if target == localPath {
		return nil // already promoted
	}
	// Sanity check: only repoint symlinks that currently point into our FUSE
	// mount. If the user has manually edited their library, we leave it alone.
	if mountPath != "" && !strings.HasPrefix(target, strings.TrimRight(mountPath, "/")+"/") && target != mountPath {
		return fmt.Errorf("library symlink %s points at %s (outside decypharr mount %s) — refusing to repoint", libFile.Path, target, mountPath)
	}
	return Repoint(libFile.Path, localPath)
}

// lockFor returns the per-entry mutex, creating it on first use.
func (o *Orchestrator) lockFor(infoHash string) *sync.Mutex {
	o.mu.Lock()
	defer o.mu.Unlock()
	lock, ok := o.locks[infoHash]
	if !ok {
		lock = &sync.Mutex{}
		o.locks[infoHash] = lock
	}
	return lock
}

func (o *Orchestrator) setStatus(infoHash string, status *Status) {
	o.mu.Lock()
	o.states[infoHash] = status
	snapshot := *status
	o.mu.Unlock()
	o.resolver.PersistStatus(infoHash, snapshot)
}

func (o *Orchestrator) updateProgress(infoHash string, status *Status) {
	if status.Total > 0 {
		status.Progress = float64(status.Bytes) / float64(status.Total)
	}
	o.setStatus(infoHash, status)
}

func (o *Orchestrator) fail(infoHash string, status *Status, err error) {
	status.State = StateFailed
	status.Error = err.Error()
	o.setStatus(infoHash, status)
	o.logger.Error().Err(err).Str("infohash", infoHash).Msg("Promotion failed")
}

// progressReader wraps an io.Reader and fires onTick after every interval
// bytes. Used to update the status snapshot without per-byte locking.
type progressReader struct {
	r        io.Reader
	onTick   func(int64)
	interval int64
	buffered int64
}

func (p *progressReader) Read(buf []byte) (int, error) {
	n, err := p.r.Read(buf)
	if n > 0 {
		p.buffered += int64(n)
		if p.buffered >= p.interval {
			p.onTick(p.buffered)
			p.buffered = 0
		}
	}
	if errors.Is(err, io.EOF) && p.buffered > 0 {
		p.onTick(p.buffered)
		p.buffered = 0
	}
	return n, err
}
