package arr

import (
	"errors"
	"fmt"
	"net/http"
	gourl "net/url"
	"path/filepath"
	"strings"
)

// LibraryFile describes one file the *arr has imported into its managed
// library. Path is the absolute filesystem path the *arr stored the file at;
// in symlink-mode setups this is typically a symlink chained back to the
// decypharr FUSE mount.
type LibraryFile struct {
	Path string
	Size int64
}

// ErrLibraryFileNotFound is returned by LibraryBridge when the *arr cannot
// confirm an imported library file for the requested release. Callers should
// treat this as "not yet importable / nothing to repoint" rather than a hard
// failure.
var ErrLibraryFileNotFound = errors.New("library file not found")

// LibraryBridge resolves the library path(s) the *arr placed for a given
// release. Implementations are per *arr type and use that *arr's HTTP API.
//
// downloadID is the qBit-style infohash decypharr surfaced when the *arr
// imported the release. fileBaseName is the basename of the source file we
// placed in decypharr's download folder — used to disambiguate when a single
// release maps to multiple library files (e.g. a season pack).
type LibraryBridge interface {
	FindLibraryFile(downloadID, fileBaseName string) (LibraryFile, error)
}

// BridgeFor returns a LibraryBridge appropriate for the *arr's type, or nil
// if the type isn't supported yet. Each new *arr just needs an additional
// case here and a small implementation.
func BridgeFor(a *Arr) LibraryBridge {
	if a == nil {
		return nil
	}
	switch a.Type {
	case Radarr:
		return &radarrBridge{arr: a}
	default:
		return nil
	}
}

// radarrBridge resolves a movie's library path via Radarr's history + movie
// APIs. The history lookup keys on the qBit-style infohash decypharr passed
// when the import happened, so it returns the exact movie that was imported
// rather than a fuzzy title match.
type radarrBridge struct {
	arr *Arr
}

type radarrHistoryRecord struct {
	ID         int    `json:"id"`
	MovieID    int    `json:"movieId"`
	EventType  string `json:"eventType"`
	DownloadID string `json:"downloadId"`
}

type radarrHistoryResponse struct {
	Records []radarrHistoryRecord `json:"records"`
}

type radarrMovieFileResponse struct {
	ID      int    `json:"id"`
	MovieID int    `json:"movieId"`
	Path    string `json:"path"`
	Size    int64  `json:"size"`
}

func (b *radarrBridge) FindLibraryFile(downloadID, _ string) (LibraryFile, error) {
	// Radarr's history downloadId is upper-cased on import, but our infohash
	// is typically lower. Try both — the *arr's filter is exact-match.
	for _, id := range []string{strings.ToLower(downloadID), strings.ToUpper(downloadID)} {
		movieID, err := b.movieIDForDownload(id)
		if err != nil {
			return LibraryFile{}, err
		}
		if movieID == 0 {
			continue
		}
		var files []radarrMovieFileResponse
		path := fmt.Sprintf("api/v3/moviefile?movieId=%d", movieID)
		resp, err := b.arr.Request(http.MethodGet, path, nil, &files)
		if err != nil {
			return LibraryFile{}, fmt.Errorf("radarr moviefile lookup: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			return LibraryFile{}, fmt.Errorf("radarr moviefile lookup: %s", resp.Status)
		}
		if len(files) == 0 {
			continue
		}
		// A movie has at most one current file; take the first.
		return LibraryFile{
			Path: filepath.Clean(files[0].Path),
			Size: files[0].Size,
		}, nil
	}
	return LibraryFile{}, ErrLibraryFileNotFound
}

// movieIDForDownload walks Radarr's history for a downloadFolderImported
// event matching this download. Returns 0 if no match.
func (b *radarrBridge) movieIDForDownload(downloadID string) (int, error) {
	q := gourl.Values{}
	q.Set("downloadId", downloadID)
	q.Set("eventType", "3") // 3 = downloadFolderImported in Radarr/Sonarr history
	q.Set("pageSize", "100")
	var data radarrHistoryResponse
	resp, err := b.arr.Request(http.MethodGet, "api/v3/history?"+q.Encode(), nil, &data)
	if err != nil {
		return 0, fmt.Errorf("radarr history lookup: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("radarr history lookup: %s", resp.Status)
	}
	for _, rec := range data.Records {
		if rec.MovieID > 0 {
			return rec.MovieID, nil
		}
	}
	return 0, nil
}
