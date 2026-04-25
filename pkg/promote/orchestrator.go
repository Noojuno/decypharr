package promote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

// MaxRetries bounds how many times the sweep will re-attempt a promotion
// before giving up. Manual triggers via Promote() reset the counter.
const MaxRetries = 5

// freeSpaceHeadroomRatio is the fraction of total entry size kept free as a
// safety margin on top of the raw byte requirement.
const freeSpaceHeadroomRatio = 0.05

// httpRetryAttempts is the total number of GET attempts per file before
// giving up. Backoff starts at 2s and doubles each attempt.
const httpRetryAttempts = 3

// importPollInterval and importPollTimeout bound the wait-for-*arr-import
// phase. We poll the *arr's library bridge instead of starting the backfill
// immediately because the *arr's mediainfo scan reads heavily through the
// FUSE mount; downloading in parallel saturates the debrid connection and
// stalls the import. Once we see the imported library file, mediainfo is
// definitionally done and the bandwidth is ours.
const (
	importPollInterval = 30 * time.Second
	importPollTimeout  = 30 * time.Minute
)

// PromoteEvent describes a transition the orchestrator wants to surface to
// the broader system (notifications, callbacks, telemetry). Wrapped behind
// a Resolver method so promote/ stays free of notification dependencies.
type PromoteEvent struct {
	InfoHash string
	Success  bool
	Message  string
	Err      error
}

// State is the lifecycle of an in-flight promotion. Held in memory for v1
// (not persisted) — schema-backed state can be added later without changing
// the orchestrator's contract.
type State string

const (
	StateIdle              State = ""
	StatePending           State = "pending"
	StateWaitingForImport  State = "waiting_for_import"
	StateDownloading       State = "downloading"
	StateRepointing        State = "repointing"
	StateComplete          State = "complete"
	StateFailed            State = "failed"
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
	// EntryFiles returns the file metadata (name, size) for an entry. The
	// download link is resolved separately just-in-time so it doesn't go
	// stale during the wait-for-import phase.
	EntryFiles(ctx context.Context, infoHash string) ([]EntryFile, error)
	// ResolveDownloadLink returns a fresh HTTP URL for downloading the named
	// file. Called immediately before the download starts.
	ResolveDownloadLink(ctx context.Context, infoHash, fileName string) (string, error)
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
	// RetriesFor returns the persisted retry count for an entry. Used to
	// short-circuit sweep attempts that have already exhausted their budget.
	RetriesFor(infoHash string) int
	// IncrementRetries bumps the persisted retry count after a failed
	// promotion attempt.
	IncrementRetries(infoHash string)
	// ResetRetries clears the persisted retry count. Called on manual
	// promote requests so users can recover after the sweep has given up.
	ResetRetries(infoHash string)
	// Notify emits a high-level event (success or failure) for the given
	// promotion. Implementation-defined sink (notifications, logs, etc.).
	Notify(event PromoteEvent)
}

// EntryFile is the per-file payload the orchestrator needs to backfill one
// item from a torrent. The download link is resolved later via
// ResolveDownloadLink so it doesn't go stale during the wait-for-import phase.
type EntryFile struct {
	Name string
	Size int64
}

// Orchestrator drives the per-entry promotion state machine. Concurrent
// promotions on the same entry are deduplicated; concurrent promotions on
// different entries run in parallel up to maxConcurrent.
type Orchestrator struct {
	cache    *Cache
	resolver Resolver
	logger   zerolog.Logger

	// sem bounds globally-concurrent backfills (auto-trigger + sweep + manual
	// all share). Prevents a 30-episode season pack from spawning 30
	// simultaneous downloads.
	sem chan struct{}

	mu     sync.Mutex
	states map[string]*Status // infohash -> live status
	locks  map[string]*sync.Mutex
}

// NewOrchestrator wires a cache + resolver into a runnable orchestrator.
// maxConcurrent is the cap on simultaneous file downloads across all
// in-flight promotions; <= 0 disables the cap.
func NewOrchestrator(cache *Cache, resolver Resolver, logger zerolog.Logger, maxConcurrent int) *Orchestrator {
	o := &Orchestrator{
		cache:    cache,
		resolver: resolver,
		logger:   logger.With().Str("component", "promote").Logger(),
		states:   make(map[string]*Status),
		locks:    make(map[string]*sync.Mutex),
	}
	if maxConcurrent > 0 {
		o.sem = make(chan struct{}, maxConcurrent)
	}
	return o
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
// the orchestrator's global semaphore (set at construction). Returns
// immediately. Safe to call repeatedly: Promote is idempotent (already-
// cached files skip download, already-repointed symlinks skip the swap),
// and the per-entry lock prevents the same entry running twice in parallel.
func (o *Orchestrator) Sweep(ctx context.Context, infoHashes []string, _ int) {
	for _, hash := range infoHashes {
		go func(h string) {
			if err := o.Promote(ctx, h); err != nil {
				o.logger.Debug().Err(err).Str("infohash", h).Msg("Sweep promote: deferred")
			}
		}(hash)
	}
}

// PromoteFromUser is the entry point for manual promote requests. Resets the
// retry counter so users can recover an entry the sweep has given up on.
func (o *Orchestrator) PromoteFromUser(ctx context.Context, infoHash string) error {
	o.resolver.ResetRetries(infoHash)
	return o.Promote(ctx, infoHash)
}

// Promote runs the full state machine for an entry: download each file into
// the local cache, then repoint the *arr library symlink at the cached file.
// Blocks until done. Per-entry mutex prevents concurrent promotions of the
// same entry; concurrent calls on different entries proceed in parallel up
// to the global semaphore limit.
func (o *Orchestrator) Promote(ctx context.Context, infoHash string) error {
	lock := o.lockFor(infoHash)
	lock.Lock()
	defer lock.Unlock()

	if retries := o.resolver.RetriesFor(infoHash); retries >= MaxRetries {
		err := fmt.Errorf("retry budget exhausted (%d attempts)", retries)
		o.logger.Debug().Str("infohash", infoHash).Int("retries", retries).Msg("Promote: skipping, retry budget exhausted")
		return err
	}

	files, err := o.resolver.EntryFiles(ctx, infoHash)
	if err != nil {
		o.fail(infoHash, &Status{State: StateFailed, Error: err.Error()}, fmt.Errorf("resolve entry files: %w", err))
		return err
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

	// Disk-space pre-flight against the cache root, including a small headroom.
	required := uint64(total) + uint64(float64(total)*freeSpaceHeadroomRatio)
	if avail, err := availableBytes(o.cache.root); err == nil {
		if avail < required {
			e := fmt.Errorf("insufficient free space at %s: need %d bytes, only %d available", o.cache.root, required, avail)
			o.fail(infoHash, status, e)
			return e
		}
	} else {
		o.logger.Warn().Err(err).Str("path", o.cache.root).Msg("Promote: could not check free disk space, proceeding anyway")
	}

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

	// Wait for the *arr to finish importing the symlink. The mediainfo step
	// reads heavily from the FUSE mount; downloading in parallel saturates
	// the debrid connection and stalls the import. Polling here keeps the
	// bandwidth clear until the *arr is done.
	status.State = StateWaitingForImport
	o.setStatus(infoHash, status)
	if err := o.waitForImport(ctx, bridge, infoHash, files); err != nil {
		o.fail(infoHash, status, err)
		return err
	}

	status.State = StateDownloading
	o.setStatus(infoHash, status)

	// Acquire the global concurrency slot only for the actual download work.
	// Holding it across the *arr API repoint stage would block other entries
	// from starting their downloads while we wait on Sonarr/Radarr.
	if o.sem != nil {
		select {
		case o.sem <- struct{}{}:
			defer func() { <-o.sem }()
		case <-ctx.Done():
			o.fail(infoHash, status, ctx.Err())
			return ctx.Err()
		}
	}

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
	o.resolver.ResetRetries(infoHash)
	o.resolver.Notify(PromoteEvent{
		InfoHash: infoHash,
		Success:  true,
		Message:  fmt.Sprintf("Promotion complete: %d file(s) now served from local disk", len(files)),
	})
	return nil
}

// waitForImport polls the *arr's library bridge until at least one of the
// entry's files has been imported, signalling the *arr is done with its
// mediainfo scan. Bounded by importPollTimeout so a never-imported entry
// doesn't block forever. Returns immediately if the library file is already
// present (e.g. manual promote of an already-imported entry).
func (o *Orchestrator) waitForImport(ctx context.Context, bridge arr.LibraryBridge, infoHash string, files []EntryFile) error {
	check := func() bool {
		for _, f := range files {
			if _, err := bridge.FindLibraryFile(infoHash, f.Name); err == nil {
				return true
			}
		}
		return false
	}
	if check() {
		return nil
	}
	o.logger.Debug().Str("infohash", infoHash).Msg("Promote: waiting for *arr to import the symlink before starting backfill")
	deadline := time.Now().Add(importPollTimeout)
	ticker := time.NewTicker(importPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if check() {
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout after %s waiting for *arr to import", importPollTimeout)
			}
		}
	}
}

// downloadOne fetches a single file via HTTP and streams it into the cache.
// Retries on transient errors (5xx, network) with exponential backoff.
func (o *Orchestrator) downloadOne(ctx context.Context, infoHash string, file EntryFile, status *Status) error {
	var lastErr error
	backoff := 2 * time.Second
	for attempt := 1; attempt <= httpRetryAttempts; attempt++ {
		err := o.downloadOnce(ctx, infoHash, file, status)
		if err == nil {
			return nil
		}
		lastErr = err
		// Context cancellation is terminal — no point retrying.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if attempt < httpRetryAttempts {
			o.logger.Warn().Err(err).Int("attempt", attempt).Str("file", file.Name).Dur("backoff", backoff).Msg("Promote download failed, retrying")
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff *= 2
		}
	}
	return fmt.Errorf("after %d attempts: %w", httpRetryAttempts, lastErr)
}

// downloadOnce performs a single GET + cache write. Resolves the download
// link just-in-time so a fresh URL is used for each attempt — important
// because debrid links can expire during the wait-for-import phase.
func (o *Orchestrator) downloadOnce(ctx context.Context, infoHash string, file EntryFile, status *Status) error {
	link, err := o.resolver.ResolveDownloadLink(ctx, infoHash, file.Name)
	if err != nil {
		return fmt.Errorf("resolve link: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
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

	startBytes := status.Bytes
	progress := &progressReader{
		r:        resp.Body,
		onTick:   func(n int64) { status.Bytes += n; o.updateProgress(infoHash, status) },
		interval: 1 << 20, // report every 1MB
	}
	if err := o.cache.Write(infoHash, file.Name, progress); err != nil {
		// Roll back the partial-byte counter so a retry doesn't double-count.
		status.Bytes = startBytes
		o.updateProgress(infoHash, status)
		return err
	}
	return nil
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
	o.resolver.IncrementRetries(infoHash)
	o.logger.Error().Err(err).Str("infohash", infoHash).Msg("Promotion failed")
	o.resolver.Notify(PromoteEvent{
		InfoHash: infoHash,
		Success:  false,
		Message:  fmt.Sprintf("Promotion failed: %s", err.Error()),
		Err:      err,
	})
}

// Cleanup removes the cache directory for an entry. If a libraryFile path is
// provided and that path is currently a symlink pointing into the cache, it's
// repointed back to the FUSE mount path before the cache is deleted (so
// playback continues to work via streaming once the local copy is gone). Used
// by the queue's delete cleanup hook so removing an entry doesn't leave
// orphan files or broken library symlinks behind.
func (o *Orchestrator) Cleanup(infoHash string, librarySymlinks []string, fuseTargetByLibPath map[string]string) error {
	cacheDir := filepath.Join(o.cache.root, infoHash)
	for _, libPath := range librarySymlinks {
		isLink, err := IsSymlink(libPath)
		if err != nil || !isLink {
			continue
		}
		target, err := ReadTarget(libPath)
		if err != nil {
			continue
		}
		if !strings.HasPrefix(target, cacheDir) {
			continue
		}
		if fuseTarget, ok := fuseTargetByLibPath[libPath]; ok && fuseTarget != "" {
			if err := Repoint(libPath, fuseTarget); err != nil {
				o.logger.Warn().Err(err).Str("library", libPath).Msg("Cleanup: failed to repoint library symlink back to FUSE — skipping cache removal to avoid breakage")
				return err
			}
		} else {
			// No FUSE fallback known — removing the cache would break the
			// library symlink. Leave the cache in place; the user can remove
			// it manually once they've fixed the library.
			o.logger.Warn().Str("library", libPath).Msg("Cleanup: no FUSE fallback target — skipping cache removal to avoid breakage")
			return nil
		}
	}
	return os.RemoveAll(cacheDir)
}

// HealLibraryLink is called by the sweep when a persisted file.LocalPath
// points at a missing cache file. Tries to repoint the library symlink (if
// any) back to the FUSE mount so playback survives, then returns. The next
// promote attempt re-downloads.
func (o *Orchestrator) HealLibraryLink(libPath, fuseTarget string) error {
	if libPath == "" || fuseTarget == "" {
		return nil
	}
	isLink, err := IsSymlink(libPath)
	if err != nil || !isLink {
		return err
	}
	target, err := ReadTarget(libPath)
	if err != nil {
		return err
	}
	// Only heal symlinks that currently point into our cache root.
	if !strings.HasPrefix(target, o.cache.root) {
		return nil
	}
	o.logger.Warn().Str("library", libPath).Str("missing_cache", target).Str("fallback", fuseTarget).Msg("Healing broken library symlink: repointing back to FUSE mount")
	return Repoint(libPath, fuseTarget)
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
