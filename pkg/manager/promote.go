package manager

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/promote"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// promoteResolver adapts Manager into the small interface the promote
// package needs. Keeping this adapter here means promote/ stays unaware of
// the broader Manager surface area — easy to test, easy to evolve.
type promoteResolver struct {
	manager *Manager
}

func (r *promoteResolver) EntryFiles(ctx context.Context, infoHash string) ([]promote.EntryFile, error) {
	entry, err := r.manager.queue.GetTorrent(infoHash)
	if err != nil {
		return nil, fmt.Errorf("get entry %s: %w", infoHash, err)
	}
	files := entry.GetActiveFiles()
	out := make([]promote.EntryFile, 0, len(files))
	for _, f := range files {
		link, err := r.manager.linkService.GetLink(ctx, entry, f.Name)
		if err != nil {
			return nil, fmt.Errorf("get download link for %s: %w", f.Name, err)
		}
		out = append(out, promote.EntryFile{
			Name:         f.Name,
			Size:         f.Size,
			DownloadLink: link.DownloadLink,
		})
	}
	return out, nil
}

func (r *promoteResolver) EntryCategory(infoHash string) (string, error) {
	entry, err := r.manager.queue.GetTorrent(infoHash)
	if err != nil {
		return "", err
	}
	return entry.Category, nil
}

func (r *promoteResolver) ArrFor(category string) *arr.Arr {
	return r.manager.arr.Get(category)
}

func (r *promoteResolver) MountPath() string {
	return r.manager.config.Mount.MountPath
}

func (r *promoteResolver) HTTPClient() *http.Client {
	return r.manager.streamClient
}

func (r *promoteResolver) PersistStatus(infoHash string, status promote.Status) {
	entry, err := r.manager.queue.GetTorrent(infoHash)
	if err != nil {
		return
	}
	entry.BackfillState = string(status.State)
	entry.BackfillBytes = status.Bytes
	entry.BackfillTotal = status.Total
	entry.BackfillError = status.Error
	_ = r.manager.queue.Update(entry)
}

func (r *promoteResolver) SetFileLocalPath(infoHash, fileName, localPath string) {
	entry, err := r.manager.queue.GetTorrent(infoHash)
	if err != nil {
		return
	}
	if entry.Files == nil {
		return
	}
	if f, ok := entry.Files[fileName]; ok {
		f.LocalPath = localPath
		_ = r.manager.queue.Update(entry)
	}
}

func (r *promoteResolver) RetriesFor(infoHash string) int {
	entry, err := r.manager.queue.GetTorrent(infoHash)
	if err != nil {
		return 0
	}
	return entry.BackfillRetries
}

func (r *promoteResolver) IncrementRetries(infoHash string) {
	entry, err := r.manager.queue.GetTorrent(infoHash)
	if err != nil {
		return
	}
	entry.BackfillRetries++
	_ = r.manager.queue.Update(entry)
}

func (r *promoteResolver) ResetRetries(infoHash string) {
	entry, err := r.manager.queue.GetTorrent(infoHash)
	if err != nil {
		return
	}
	if entry.BackfillRetries == 0 {
		return
	}
	entry.BackfillRetries = 0
	_ = r.manager.queue.Update(entry)
}

func (r *promoteResolver) Notify(event promote.PromoteEvent) {
	if r.manager.Notifications == nil {
		return
	}
	entry, _ := r.manager.queue.GetTorrent(event.InfoHash)
	notifyType := config.EventPromoteComplete
	status := "success"
	if !event.Success {
		notifyType = config.EventPromoteFailed
		status = "error"
	}
	r.manager.Notifications.Notify(notifications.Event{
		Type:    notifyType,
		Status:  status,
		Entry:   entry,
		Message: event.Message,
		Error:   event.Err,
	})
}

// Promoter returns the orchestrator for this Manager. Lazy so tests that
// don't exercise promotion don't pay setup cost; safe to call repeatedly.
func (m *Manager) Promoter() *promote.Orchestrator {
	m.promoterOnce.Do(func() {
		concurrency := m.config.MaxDownloads
		if concurrency <= 0 {
			concurrency = 2
		}
		cache := promote.NewCache(config.Get().LocalFilesPath)
		m.promoter = promote.NewOrchestrator(cache, &promoteResolver{manager: m}, m.logger, concurrency)
	})
	return m.promoter
}

// promoteCleanup unwinds a promoted entry: repoints any library symlinks
// that target our cache back to the FUSE mount, then deletes the cache
// directory. Safe to call for entries that were never promoted (no cache,
// no broken symlinks). Called from the queue's Delete cleanup path.
func (m *Manager) promoteCleanup(entry *storage.Entry) {
	if entry == nil {
		return
	}
	bridge := arr.BridgeFor(m.arr.Get(entry.Category))
	var libSymlinks []string
	fuseTarget := make(map[string]string)
	for _, f := range entry.Files {
		if f == nil || f.LocalPath == "" {
			continue
		}
		if bridge != nil {
			if libFile, err := bridge.FindLibraryFile(entry.InfoHash, f.Name); err == nil && libFile.Path != "" {
				libSymlinks = append(libSymlinks, libFile.Path)
				// FUSE fallback target = decypharr mount + entry folder + filename.
				// Best-effort; if MountPath is empty we just skip the repoint.
				if mp := m.config.Mount.MountPath; mp != "" {
					fuseTarget[libFile.Path] = filepath.Join(mp, "__all__", entry.GetFolder(), f.Name)
				}
			}
		}
	}
	if err := m.Promoter().Cleanup(entry.InfoHash, libSymlinks, fuseTarget); err != nil {
		m.logger.Warn().Err(err).Str("infohash", entry.InfoHash).Msg("Promote cleanup encountered an error")
	}
}

// promoteSweep finds entries that need promotion attention and dispatches
// them through the orchestrator. Called both at startup and periodically.
// The work it does per entry:
//
//   - Self-heal: if a file's persisted LocalPath references a missing cache
//     file AND the *arr library symlink still points at that missing path,
//     repoint the library symlink back to the FUSE mount so playback isn't
//     silently broken while we wait to re-download. Then clear LocalPath so
//     the next attempt re-downloads.
//   - Skip entries that have exhausted their retry budget — they can be
//     manually retried via the API endpoint, which resets the counter.
//   - Dispatch the rest through the orchestrator. Promote is idempotent, so
//     re-runs are safe and cheap.
func (m *Manager) promoteSweep(ctx context.Context) {
	if !m.config.AutoPromote {
		return
	}
	entries := m.queue.ListFilter("", config.ProtocolAll, "", nil, "", false)
	var toPromote []string
	for _, entry := range entries {
		if entry.Action != config.DownloadActionSymlink {
			continue
		}
		bridge := arr.BridgeFor(m.arr.Get(entry.Category))
		// Self-heal stale LocalPath records pointing at missing cache files.
		for _, file := range entry.Files {
			if file == nil || file.LocalPath == "" {
				continue
			}
			if _, err := os.Stat(file.LocalPath); !os.IsNotExist(err) {
				continue
			}
			// Cache file is gone. Try to repoint the library symlink back to
			// FUSE so playback survives until the next promote completes.
			if bridge != nil && m.config.Mount.MountPath != "" {
				if libFile, err := bridge.FindLibraryFile(entry.InfoHash, file.Name); err == nil && libFile.Path != "" {
					fuseTarget := filepath.Join(m.config.Mount.MountPath, "__all__", entry.GetFolder(), file.Name)
					if err := m.Promoter().HealLibraryLink(libFile.Path, fuseTarget); err != nil {
						m.logger.Warn().Err(err).Str("library", libFile.Path).Msg("Promote sweep: failed to heal library symlink")
					}
				}
			}
			file.LocalPath = ""
			_ = m.queue.Update(entry)
		}
		if entry.BackfillState == string(promote.StateComplete) {
			continue
		}
		if entry.BackfillRetries >= promote.MaxRetries {
			continue // exhausted; user can manually retry via API
		}
		toPromote = append(toPromote, entry.InfoHash)
	}
	if len(toPromote) == 0 {
		return
	}
	m.logger.Debug().Int("count", len(toPromote)).Msg("Promote sweep: dispatching candidates")
	m.Promoter().Sweep(ctx, toPromote, 0)
}
