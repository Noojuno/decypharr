package manager

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/promote"
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

// Promoter returns the orchestrator for this Manager. Lazy so tests that
// don't exercise promotion don't pay setup cost; safe to call repeatedly.
func (m *Manager) Promoter() *promote.Orchestrator {
	m.promoterOnce.Do(func() {
		cache := promote.NewCache(config.Get().LocalFilesPath)
		m.promoter = promote.NewOrchestrator(cache, &promoteResolver{manager: m}, m.logger)
	})
	return m.promoter
}

// promoteSweep finds entries that need promotion attention and dispatches
// them through the orchestrator. Called both at startup and periodically:
//   - Symlink-mode entries that completed but never finished promotion
//     (BackfillState != complete) get retried.
//   - Entries where a file's persisted LocalPath references a missing cache
//     file are reset (LocalPath cleared) so the next promotion attempt
//     downloads them again rather than referencing a ghost.
//
// The orchestrator itself is idempotent, so the worst case for spurious
// entries is a few cheap "no-op" library lookups.
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
		// Heal stale LocalPath records that point at missing cache files.
		for _, file := range entry.Files {
			if file == nil || file.LocalPath == "" {
				continue
			}
			if _, err := os.Stat(file.LocalPath); os.IsNotExist(err) {
				file.LocalPath = ""
				_ = m.queue.Update(entry)
			}
		}
		if entry.BackfillState != string(promote.StateComplete) {
			toPromote = append(toPromote, entry.InfoHash)
		}
	}
	if len(toPromote) == 0 {
		return
	}
	m.logger.Debug().Int("count", len(toPromote)).Msg("Promote sweep: dispatching candidates")
	concurrency := m.config.MaxDownloads
	if concurrency <= 0 {
		concurrency = 2
	}
	m.Promoter().Sweep(ctx, toPromote, concurrency)
}
