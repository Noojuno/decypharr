package manager

import (
	"context"
	"fmt"
	"net/http"

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

// Promoter returns the orchestrator for this Manager. Lazy so tests that
// don't exercise promotion don't pay setup cost; safe to call repeatedly.
func (m *Manager) Promoter() *promote.Orchestrator {
	m.promoterOnce.Do(func() {
		cache := promote.NewCache(config.Get().LocalFilesPath)
		m.promoter = promote.NewOrchestrator(cache, &promoteResolver{manager: m}, m.logger)
	})
	return m.promoter
}
