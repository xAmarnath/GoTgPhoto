package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gpix/pkg/bridge"
	"gpix/pkg/disguise"
	"gpix/pkg/fusefs"
	"gpix/pkg/gpmc"
	"gpix/pkg/vfs"
)

type TransferJob struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"` // upload | tg-import | tg-queue | tg-send
	Name       string `json:"name"`
	Stage      string `json:"stage"`
	Status     string `json:"status"` // running | done | error
	BytesDone  int64  `json:"bytesDone"`
	BytesTotal int64  `json:"bytesTotal"`
	MediaKey   string `json:"mediaKey,omitempty"`
	Error      string `json:"error,omitempty"`
	StartedAt  int64  `json:"startedAt"`
	UpdatedAt  int64  `json:"updatedAt"`
}

type jobManager struct {
	mu   sync.Mutex
	jobs []*TransferJob
	byID map[string]*TransferJob
}

func newJobManager() *jobManager {
	return &jobManager{byID: map[string]*TransferJob{}}
}

func (jm *jobManager) create(kind, name string, total int64) *TransferJob {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	now := time.Now().UnixMilli()
	j := &TransferJob{
		ID:         randID(),
		Kind:       kind,
		Name:       name,
		Stage:      "starting",
		Status:     "running",
		BytesTotal: total,
		StartedAt:  now,
		UpdatedAt:  now,
	}
	jm.jobs = append([]*TransferJob{j}, jm.jobs...)
	jm.byID[j.ID] = j
	if len(jm.jobs) > 60 {
		for _, old := range jm.jobs[60:] {
			delete(jm.byID, old.ID)
		}
		jm.jobs = jm.jobs[:60]
	}
	return j
}

func (jm *jobManager) update(id, stage string, done, total int64) {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	if j, ok := jm.byID[id]; ok {
		if stage != "" {
			j.Stage = stage
		}
		if done >= 0 {
			j.BytesDone = done
		}
		if total > 0 {
			j.BytesTotal = total
		}
		j.UpdatedAt = time.Now().UnixMilli()
	}
}

func (jm *jobManager) finish(id, mediaKey string, err error) {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	if j, ok := jm.byID[id]; ok {
		j.UpdatedAt = time.Now().UnixMilli()
		if err != nil {
			j.Status = "error"
			j.Stage = "failed"
			j.Error = err.Error()
		} else {
			j.Status = "done"
			j.Stage = "completed"
			j.MediaKey = mediaKey
			if j.BytesTotal > 0 {
				j.BytesDone = j.BytesTotal
			}
		}
	}
}

func (jm *jobManager) snapshot() []TransferJob {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	out := make([]TransferJob, len(jm.jobs))
	for i, j := range jm.jobs {
		out[i] = *j
	}
	return out
}

func (s *Server) registerAPIV1(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/status", s.requireAPI(s.handleV1Status))
	mux.HandleFunc("GET /api/v1/files", s.requireAPI(s.handleV1Files))
	mux.HandleFunc("POST /api/v1/delete", s.requireAPI(s.handleV1Delete))
	mux.HandleFunc("POST /api/v1/rename", s.requireAPI(s.handleV1Rename))
	mux.HandleFunc("POST /api/v1/upload", s.requireAPI(s.handleV1Upload))
	mux.HandleFunc("GET /api/v1/jobs", s.requireAPI(s.handleV1Jobs))
	mux.HandleFunc("GET /api/v1/bulk/download", s.requireAPI(s.handleBulkDownload))

	mux.HandleFunc("GET /api/v1/albums", s.requireAPI(s.handleAlbumsList))
	mux.HandleFunc("POST /api/v1/albums", s.requireAPI(s.handleAlbumCreate))
	mux.HandleFunc("POST /api/v1/albums/add", s.requireAPI(s.handleAlbumAdd))
	mux.HandleFunc("POST /api/v1/albums/remove", s.requireAPI(s.handleV1AlbumRemove))
	mux.HandleFunc("DELETE /api/v1/albums/{key}", s.requireAPI(s.handleV1AlbumDelete))

	mux.HandleFunc("GET /api/v1/media/{key}/info", s.requireAPI(s.handleV1MediaInfo))
	mux.HandleFunc("GET /api/v1/media/{key}/thumb", s.requireAPI(func(w http.ResponseWriter, r *http.Request) {
		s.serveThumbByKey(w, r, r.PathValue("key"))
	}))
	mux.HandleFunc("GET /api/v1/media/{key}/stream", s.requireAPI(func(w http.ResponseWriter, r *http.Request) {
		s.serveStreamByKey(w, r, r.PathValue("key"))
	}))
	mux.HandleFunc("HEAD /api/v1/media/{key}/stream", s.requireAPI(func(w http.ResponseWriter, r *http.Request) {
		s.serveStreamByKey(w, r, r.PathValue("key"))
	}))
	mux.HandleFunc("GET /api/v1/media/{key}/raw", s.requireAPI(func(w http.ResponseWriter, r *http.Request) {
		s.serveRangeMedia(w, r, r.PathValue("key"), true)
	}))
	mux.HandleFunc("HEAD /api/v1/media/{key}/raw", s.requireAPI(func(w http.ResponseWriter, r *http.Request) {
		s.serveRangeMedia(w, r, r.PathValue("key"), true)
	}))

	mux.HandleFunc("GET /api/v1/vfs", s.requireAPI(s.handleV1VFSList))
	mux.HandleFunc("POST /api/v1/vfs/mkdir", s.requireAPI(s.handleV1VFSMkdir))
	mux.HandleFunc("POST /api/v1/vfs/rename", s.requireAPI(s.handleV1VFSRename))
	mux.HandleFunc("POST /api/v1/vfs/chmod", s.requireAPI(s.handleV1VFSChmod))
	mux.HandleFunc("DELETE /api/v1/vfs", s.requireAPI(s.handleV1VFSDelete))

	mux.HandleFunc("GET /api/v1/telegram/status", s.requireAPI(s.handleV1TGStatus))
	mux.HandleFunc("POST /api/v1/telegram/import", s.requireAPI(s.handleV1TGImport))
	mux.HandleFunc("POST /api/v1/telegram/queue/upload", s.requireAPI(s.handleV1TGQueueUpload))
	mux.HandleFunc("POST /api/v1/telegram/queue/clear", s.requireAPI(s.handleV1TGQueueClear))
	mux.HandleFunc("POST /api/v1/telegram/autoupload", s.requireAPI(s.handleV1TGAutoUpload))
	mux.HandleFunc("POST /api/v1/telegram/send", s.requireAPI(s.handleV1TGSend))

	mux.HandleFunc("GET /api/v1/config", s.requireAPI(s.handleV1ConfigGet))
	mux.HandleFunc("POST /api/v1/config", s.requireAPI(s.handleV1ConfigSave))
	mux.HandleFunc("POST /api/v1/config/test-token", s.requireAPI(s.handleV1TestToken))

	mux.HandleFunc("POST /api/v1/fuse/mount", s.requireAPI(s.handleV1FUSEMount))
	mux.HandleFunc("POST /api/v1/fuse/unmount", s.requireAPI(s.handleV1FUSEUnmount))
}

type fileDTO struct {
	MediaKey    string `json:"mediaKey"`
	Filename    string `json:"filename"`
	DisplayName string `json:"displayName"`
	Kind        string `json:"kind"` // photo | video | audio | document
	DisplayKind string `json:"displayKind"`
	IsDisguised bool   `json:"isDisguised"`
	IsEncrypted bool   `json:"isEncrypted"`
	SizeBytes   int64  `json:"sizeBytes"`
	MtimeMs     int64  `json:"mtimeMs"`
}

func (s *Server) toFileDTO(it gpmc.MediaItem) fileDTO {
	v := s.vfsInstance()
	dispName := v.LogicalName(it)
	size := v.LogicalSize(it)
	mtime := v.LogicalModTime(it)

	isDisguised := false
	isEncrypted := false
	if dm, ok := v.Meta().DisguiseFor(it.MediaKey, it.SHA1); ok && dm.Version > 0 {
		isDisguised = true
		isEncrypted = dm.Version == 2
	} else if _, ok := disguise.LooksLikeDisguisedFilename(it.Filename); ok {
		isDisguised = true
	}

	kind := "document"
	displayKind := describeKindForFilename(dispName)
	ext := strings.ToLower(filepath.Ext(dispName))
	switch {
	case !isDisguised && it.Kind == gpmc.KindPhoto:
		kind = "photo"
		displayKind = "Photo"
	case !isDisguised && it.Kind == gpmc.KindVideo:
		kind = "video"
		displayKind = "Video"
	case ext == ".mp4" || ext == ".mkv" || ext == ".webm" || ext == ".mov" || ext == ".avi":
		kind = "video"
		if displayKind == "" {
			displayKind = "Video"
		}
	case ext == ".mp3" || ext == ".flac" || ext == ".wav" || ext == ".m4a" || ext == ".ogg" || ext == ".opus":
		kind = "audio"
		displayKind = "Audio"
	case ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".webp" || ext == ".gif" || ext == ".avif":
		kind = "photo"
		displayKind = "Image"
	}

	return fileDTO{
		MediaKey:    it.MediaKey,
		Filename:    it.Filename,
		DisplayName: dispName,
		Kind:        kind,
		DisplayKind: displayKind,
		IsDisguised: isDisguised,
		IsEncrypted: isEncrypted,
		SizeBytes:   size,
		MtimeMs:     mtime.UnixMilli(),
	}
}

func (s *Server) handleV1Status(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	gp := s.client()
	v := s.vfsInstance()

	quota, _ := v.Statfs(ctx)
	cachedBlocks, activeStaged := v.CacheStats()

	items, listErr := gp.ListAllItems(ctx)
	var totalLibBytes int64
	var visibleCount int
	var disguisedCount int
	for _, it := range items {
		if v.Meta().IsHidden(it.MediaKey) {
			continue
		}
		visibleCount++
		totalLibBytes += v.LogicalSize(it)
		if dm, ok := v.Meta().DisguiseFor(it.MediaKey, it.SHA1); (ok && dm.Version > 0) || hasDisguiseSuffix(it.Filename) {
			disguisedCount++
		}
	}

	albums, _ := gp.ListAlbums(ctx)

	var fuseSt fusefs.Status
	fuseSt.Supported = fusefs.Supported
	s.mu.RLock()
	getFUSE := s.hooks.GetFUSEStatus
	getBot := s.hooks.GetBot
	_, hasWebDAV := s.extraMounts["/dav/"]
	_, hasS3 := s.extraMounts["/s3/"]
	s.mu.RUnlock()

	if getFUSE != nil {
		fuseSt = getFUSE()
	}

	var tgSt bridge.Status
	tgSt.Queue = []bridge.QueuedItemDTO{}
	if getBot != nil {
		if b := getBot(); b != nil {
			tgSt = b.StatusSnapshot()
		}
	}

	email := ""
	if parsed, err := url.ParseQuery(gp.AuthData()); err == nil {
		email = parsed.Get("Email")
	}
	prof := gp.DeviceProfile()
	authOK := listErr == nil

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":    authOK,
		"error": errStr(listErr),
		"account": map[string]any{
			"email":   email,
			"model":   prof.Model,
			"profile": s.cfg.DeviceProfile,
		},
		"quota": quota,
		"library": map[string]any{
			"totalItems":     visibleCount,
			"totalBytes":     totalLibBytes,
			"albumCount":     len(albums),
			"disguisedItems": disguisedCount,
		},
		"fuse": map[string]any{
			"supported":    fuseSt.Supported,
			"mounted":      fuseSt.Mounted,
			"mountPoint":   fuseSt.MountPoint,
			"allowOther":   fuseSt.AllowOther,
			"error":        fuseSt.Error,
			"cachedBlocks": cachedBlocks,
			"activeStaged": activeStaged,
		},
		"telegram": tgSt,
		"services": map[string]any{
			"webdav": hasWebDAV,
			"s3":     hasS3,
		},
		"jobs": s.jobs.snapshot(),
	})
}

func hasDisguiseSuffix(name string) bool {
	_, ok := disguise.LooksLikeDisguisedFilename(name)
	return ok
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (s *Server) handleV1Files(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	gp := s.client()
	v := s.vfsInstance()

	if r.URL.Query().Get("refresh") == "1" {
		gp.InvalidateSyncCache()
	}

	albumKey := strings.TrimSpace(r.URL.Query().Get("album"))
	var raw []gpmc.MediaItem
	var err error
	if albumKey != "" {
		raw, err = gp.ListAlbumItems(ctx, albumKey)
	} else {
		raw, err = gp.ListAllItems(ctx)
	}
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}

	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	kindFilter := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("kind")))
	sortBy := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort")))
	order := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("order")))
	asc := order == "asc"

	files := make([]fileDTO, 0, len(raw))
	for _, it := range raw {
		if v.Meta().IsHidden(it.MediaKey) {
			continue
		}
		dto := s.toFileDTO(it)
		if q != "" && !strings.Contains(strings.ToLower(dto.DisplayName), q) && !strings.Contains(strings.ToLower(dto.MediaKey), q) {
			continue
		}
		switch kindFilter {
		case "", "all":
		case "disguised":
			if !dto.IsDisguised {
				continue
			}
		case "encrypted":
			if !dto.IsEncrypted {
				continue
			}
		default:
			if dto.Kind != kindFilter {
				continue
			}
		}
		files = append(files, dto)
	}

	sort.Slice(files, func(i, j int) bool {
		var cmp int
		switch sortBy {
		case "name":
			cmp = strings.Compare(strings.ToLower(files[i].DisplayName), strings.ToLower(files[j].DisplayName))
		case "size":
			if files[i].SizeBytes < files[j].SizeBytes {
				cmp = -1
			} else if files[i].SizeBytes > files[j].SizeBytes {
				cmp = 1
			}
		case "kind", "type":
			cmp = strings.Compare(files[i].DisplayKind, files[j].DisplayKind)
		default: // mtime
			if files[i].MtimeMs < files[j].MtimeMs {
				cmp = -1
			} else if files[i].MtimeMs > files[j].MtimeMs {
				cmp = 1
			}
			if sortBy == "" && order == "" {
				// Default mtime is newest-first.
				return cmp > 0
			}
		}
		if cmp == 0 {
			cmp = strings.Compare(files[i].MediaKey, files[j].MediaKey)
		}
		if asc {
			return cmp < 0
		}
		return cmp > 0
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"files": files,
		"total": len(files),
	})
}

func (s *Server) handleV1Delete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Keys      []string `json:"keys"`
		AlbumKey  string   `json:"albumKey"`
		Permanent bool     `json:"permanent"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	keys := dedupeNonEmpty(req.Keys)
	if len(keys) == 0 {
		writeJSONError(w, http.StatusBadRequest, "keys is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	gp := s.client()
	v := s.vfsInstance()

	if req.AlbumKey != "" {
		if store := gp.AlbumStore(); store != nil {
			if err := store.RemoveMembers(ctx, req.AlbumKey, keys); err != nil {
				writeJSONError(w, http.StatusBadGateway, err.Error())
				return
			}
			gp.InvalidateSyncCache()
			writeJSON(w, http.StatusOK, map[string]any{"removedFromAlbum": len(keys)})
			return
		}
		writeJSONError(w, http.StatusBadRequest, "album store unavailable")
		return
	}

	results, err := gp.DeleteByMediaKeys(ctx, keys, req.Permanent)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}
	errs := map[string]string{}
	deleted := 0
	for _, k := range keys {
		if e := results[k]; e != nil {
			errs[k] = e.Error()
		} else {
			deleted++
			v.InvalidateMedia(k)
			v.Meta().HideKey(k)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"deleted": deleted,
		"errors":  errs,
	})
}

func (s *Server) handleV1Rename(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MediaKey string `json:"mediaKey"`
		NewName  string `json:"newName"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	req.MediaKey = strings.TrimSpace(req.MediaKey)
	req.NewName = strings.TrimSpace(strings.ReplaceAll(req.NewName, "/", "-"))
	if req.MediaKey == "" || req.NewName == "" {
		writeJSONError(w, http.StatusBadRequest, "mediaKey and newName are required")
		return
	}
	s.vfsInstance().Meta().SetNameOverride(req.MediaKey, req.NewName)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "mediaKey": req.MediaKey, "displayName": req.NewName})
}

func (s *Server) handleV1AlbumRemove(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AlbumMediaKey string   `json:"album_media_key"`
		MediaKeys     []string `json:"media_keys"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 256*1024)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	gp := s.client()
	if store := gp.AlbumStore(); store != nil {
		if err := store.RemoveMembers(r.Context(), req.AlbumMediaKey, req.MediaKeys); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		gp.InvalidateSyncCache()
		writeJSON(w, http.StatusOK, map[string]any{"removed": len(req.MediaKeys)})
		return
	}
	writeJSONError(w, http.StatusBadRequest, "album store unavailable")
}

func (s *Server) handleV1AlbumDelete(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.PathValue("key"))
	if key == "" {
		writeJSONError(w, http.StatusBadRequest, "missing album key")
		return
	}
	gp := s.client()
	if store := gp.AlbumStore(); store != nil {
		if err := store.DeleteAlbum(r.Context(), key); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		gp.InvalidateSyncCache()
		writeJSON(w, http.StatusOK, map[string]any{"deleted": key})
		return
	}
	writeJSONError(w, http.StatusBadRequest, "album store unavailable")
}

func (s *Server) handleV1Upload(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})

	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)
	mr, err := r.MultipartReader()
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "multipart: "+err.Error())
		return
	}

	quality := gpmc.QualityOriginal
	albumKey := ""
	passphrase := os.Getenv("GPIX_ENC_PASSPHRASE")
	encryptOn := false

	type uploadedFile struct {
		Name        string `json:"name"`
		MediaKey    string `json:"mediaKey"`
		Skipped     bool   `json:"skipped"`
		IsDisguised bool   `json:"isDisguised"`
		IsEncrypted bool   `json:"isEncrypted"`
	}
	var uploaded []uploadedFile
	var uploadErrors []string

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "part: "+err.Error())
			return
		}

		switch part.FormName() {
		case "quality":
			b, _ := io.ReadAll(io.LimitReader(part, 64))
			switch strings.TrimSpace(string(b)) {
			case "saver":
				quality = gpmc.QualitySaver
			case "quota":
				quality = gpmc.QualityUseQuota
			default:
				quality = gpmc.QualityOriginal
			}
		case "albumKey", "album_key":
			b, _ := io.ReadAll(io.LimitReader(part, 256))
			albumKey = strings.TrimSpace(string(b))
		case "encrypt":
			b, _ := io.ReadAll(io.LimitReader(part, 16))
			v := strings.TrimSpace(string(b))
			encryptOn = v == "on" || v == "1" || v == "true"
		case "passphrase":
			b, _ := io.ReadAll(io.LimitReader(part, 1024))
			if v := strings.TrimSpace(string(b)); v != "" {
				passphrase = v
			}
		case "files", "file":
			fn := filepath.Base(part.FileName())
			if fn == "" || fn == "." {
				_, _ = io.Copy(io.Discard, part)
				continue
			}
			pw := ""
			if encryptOn {
				if passphrase == "" {
					uploadErrors = append(uploadErrors, fn+": encryption requested without passphrase")
					_, _ = io.Copy(io.Discard, part)
					continue
				}
				pw = passphrase
			}
			up, uerr := s.uploadStreamV1(r.Context(), part, fn, albumKey, quality, pw)
			if uerr != nil {
				uploadErrors = append(uploadErrors, fn+": "+uerr.Error())
				continue
			}
			uploaded = append(uploaded, uploadedFile{
				Name:        fn,
				MediaKey:    up.mediaKey,
				Skipped:     up.skipped,
				IsDisguised: up.disguised,
				IsEncrypted: up.encrypted,
			})
		default:
			_, _ = io.Copy(io.Discard, part)
		}
	}

	status := http.StatusOK
	if len(uploaded) == 0 && len(uploadErrors) > 0 {
		status = http.StatusBadGateway
	}
	writeJSON(w, status, map[string]any{
		"uploaded": uploaded,
		"errors":   uploadErrors,
	})
}

type uploadOutcome struct {
	mediaKey  string
	skipped   bool
	disguised bool
	encrypted bool
}

func (s *Server) uploadStreamV1(ctx context.Context, src io.Reader, name, albumKey string, quality gpmc.Quality, passphrase string) (uploadOutcome, error) {
	job := s.jobs.create("upload", name, 0)
	var finalKey string
	var finalErr error
	defer func() {
		s.jobs.finish(job.ID, finalKey, finalErr)
	}()

	select {
	case s.tempSemaphore <- struct{}{}:
	case <-ctx.Done():
		finalErr = ctx.Err()
		return uploadOutcome{}, finalErr
	}
	defer func() { <-s.tempSemaphore }()

	if err := os.MkdirAll(s.cfg.TempDir, 0o700); err != nil {
		finalErr = err
		return uploadOutcome{}, err
	}
	tf, err := os.CreateTemp(s.cfg.TempDir, "gpix-v1-*"+filepath.Ext(name))
	if err != nil {
		finalErr = err
		return uploadOutcome{}, err
	}
	tmpPath := tf.Name()
	defer os.Remove(tmpPath)

	s.jobs.update(job.ID, "receiving upload", 0, 0)
	written, err := io.Copy(tf, src)
	_ = tf.Close()
	if err != nil {
		finalErr = err
		return uploadOutcome{}, err
	}
	s.jobs.update(job.ID, "preparing", written, written)

	uploadPath := tmpPath
	commitName := name
	out := uploadOutcome{}

	head, _ := readHead(tmpPath, 512)
	if written == 0 || disguise.ShouldWrap("", name, head) || passphrase != "" {
		s.jobs.update(job.ID, "wrapping as MP4", 0, written)
		wrappedPath, werr := wrapToTemp(s.cfg.TempDir, tmpPath, name, passphrase)
		if werr != nil {
			finalErr = werr
			return uploadOutcome{}, werr
		}
		defer os.Remove(wrappedPath)
		uploadPath = wrappedPath
		commitName = name + ".mp4"
		out.disguised = true
		out.encrypted = passphrase != ""
	}

	gp := s.client()
	v := s.vfsInstance()

	res, err := gp.UploadFileWithProgress(ctx, uploadPath, gpmc.UploadOpts{
		Quality:      quality,
		OverrideName: commitName,
	}, func(ev gpmc.UploadEvent) {
		s.jobs.update(job.ID, ev.Stage.String(), ev.BytesDone, ev.BytesTotal)
	})
	if err != nil {
		finalErr = err
		return uploadOutcome{}, err
	}
	out.mediaKey = res.MediaKey
	out.skipped = res.Skipped
	finalKey = res.MediaKey
	v.Meta().UnhideKey(res.MediaKey)

	if out.disguised {
		if wf, oerr := os.Open(uploadPath); oerr == nil {
			var wHead [64 * 1024]byte
			wn, _ := io.ReadFull(wf, wHead[:])
			_ = wf.Close()
			if ver, hdr, pOff, perr := disguise.PeekMeta(wHead[:wn]); perr == nil && ver > 0 {
				v.Meta().PutDisguise(res.MediaKey, vfs.DisguiseMeta{
					Checked:       true,
					Version:       ver,
					OriginalName:  hdr.Filename,
					PayloadSize:   hdr.PayloadSize,
					PayloadOffset: int64(pOff),
				})
			}
		}
	} else {
		v.Meta().PutDisguise(res.MediaKey, vfs.DisguiseMeta{
			Checked:     true,
			Version:     0,
			PayloadSize: written,
		})
	}

	if albumKey != "" && res.MediaKey != "" {
		_ = gp.AddMediaToAlbum(ctx, albumKey, []string{res.MediaKey})
		if store := gp.AlbumStore(); store != nil {
			_ = store.AddMembers(ctx, albumKey, []string{res.MediaKey})
		}
	}
	return out, nil
}

func (s *Server) handleV1Jobs(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"jobs": s.jobs.snapshot()})
}

func (s *Server) handleV1MediaInfo(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.PathValue("key"))
	if key == "" {
		writeJSONError(w, http.StatusBadRequest, "missing media key")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	gp := s.client()
	v := s.vfsInstance()

	it, found, _ := gp.GetItem(ctx, key)
	if !found {
		it = gpmc.MediaItem{MediaKey: key}
	}
	_, _ = v.InspectMedia(ctx, it)
	dto := s.toFileDTO(it)

	var hlsVariants []map[string]any
	if dto.Kind == "video" && !dto.IsDisguised {
		if manifest, err := gp.GetStreamManifest(ctx, key, "hls"); err == nil {
			for _, vr := range ParseMasterPlaylist(manifest) {
				hlsVariants = append(hlsVariants, map[string]any{
					"index":     vr.Index,
					"label":     qualityLabel(vr),
					"width":     vr.Width,
					"height":    vr.Height,
					"bandwidth": vr.Bandwidth,
				})
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"file":        dto,
		"hlsVariants": hlsVariants,
	})
}

func (s *Server) handleV1VFSList(w http.ResponseWriter, r *http.Request) {
	p := vfs.CleanPath(r.URL.Query().Get("path"))
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()

	nodes, err := s.vfsInstance().ReadDir(ctx, p)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	type entry struct {
		Name          string `json:"name"`
		Path          string `json:"path"`
		IsDir         bool   `json:"isDir"`
		IsSymlink     bool   `json:"isSymlink"`
		SymlinkTarget string `json:"symlinkTarget,omitempty"`
		SizeBytes     int64  `json:"sizeBytes"`
		Mode          string `json:"mode"`
		ModeOctal     string `json:"modeOctal"`
		UID           uint32 `json:"uid"`
		GID           uint32 `json:"gid"`
		Nlink         uint32 `json:"nlink"`
		MtimeMs       int64  `json:"mtimeMs"`
		MediaKey      string `json:"mediaKey,omitempty"`
	}
	out := make([]entry, len(nodes))
	for i, n := range nodes {
		full := p + "/" + n.FileName
		if p == "/" {
			full = "/" + n.FileName
		}
		out[i] = entry{
			Name:          n.FileName,
			Path:          full,
			IsDir:         n.IsDirFlag,
			IsSymlink:     n.FileMode&fs.ModeSymlink != 0,
			SymlinkTarget: n.SymlinkTarget,
			SizeBytes:     n.FileSize,
			Mode:          n.FileMode.String(),
			ModeOctal:     fmt.Sprintf("%04o", n.FileMode.Perm()),
			UID:           n.UID,
			GID:           n.GID,
			Nlink:         n.Nlink,
			MtimeMs:       n.Mtime.UnixMilli(),
			MediaKey:      n.MediaKey,
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"path": p, "entries": out})
}

func (s *Server) handleV1VFSMkdir(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 16*1024)).Decode(&req) != nil || strings.TrimSpace(req.Path) == "" {
		writeJSONError(w, http.StatusBadRequest, "path is required")
		return
	}
	if err := s.vfsInstance().Mkdir(r.Context(), req.Path, 0o755); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleV1VFSRename(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OldPath string `json:"oldPath"`
		NewPath string `json:"newPath"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 16*1024)).Decode(&req) != nil || req.OldPath == "" || req.NewPath == "" {
		writeJSONError(w, http.StatusBadRequest, "oldPath and newPath are required")
		return
	}
	if err := s.vfsInstance().Rename(r.Context(), req.OldPath, req.NewPath); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleV1VFSChmod(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
		Mode string `json:"mode"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 16*1024)).Decode(&req) != nil || req.Path == "" || req.Mode == "" {
		writeJSONError(w, http.StatusBadRequest, "path and octal mode are required")
		return
	}
	m, err := strconv.ParseUint(req.Mode, 8, 32)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid octal mode")
		return
	}
	if err := s.vfsInstance().Chmod(r.Context(), req.Path, fs.FileMode(m)); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleV1VFSDelete(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimSpace(r.URL.Query().Get("path"))
	if p == "" || p == "/" {
		writeJSONError(w, http.StatusBadRequest, "valid path is required")
		return
	}
	if err := s.vfsInstance().RemoveAll(r.Context(), p); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) currentBot() *bridge.Bot {
	s.mu.RLock()
	fn := s.hooks.GetBot
	s.mu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn()
}

func (s *Server) handleV1TGStatus(w http.ResponseWriter, _ *http.Request) {
	b := s.currentBot()
	if b == nil {
		writeJSON(w, http.StatusOK, bridge.Status{Queue: []bridge.QueuedItemDTO{}})
		return
	}
	writeJSON(w, http.StatusOK, b.StatusSnapshot())
}

func (s *Server) handleV1TGImport(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Spec       string `json:"spec"`
		AlbumKey   string `json:"albumKey"`
		Quality    string `json:"quality"`
		Encrypt    bool   `json:"encrypt"`
		Passphrase string `json:"passphrase"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	req.Spec = strings.TrimSpace(req.Spec)
	if req.Spec == "" {
		writeJSONError(w, http.StatusBadRequest, "Telegram message link or ID is required")
		return
	}
	b := s.currentBot()
	if b == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "Telegram bot is not running; configure TG_* settings in Setup")
		return
	}

	q := gpmc.QualityOriginal
	switch req.Quality {
	case "saver":
		q = gpmc.QualitySaver
	case "quota":
		q = gpmc.QualityUseQuota
	}
	pw := ""
	if req.Encrypt {
		pw = req.Passphrase
		if pw == "" {
			pw = os.Getenv("GPIX_ENC_PASSPHRASE")
		}
		if pw == "" {
			writeJSONError(w, http.StatusBadRequest, "encryption passphrase is required")
			return
		}
	}

	job := s.jobs.create("tg-import", req.Spec, 0)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
		defer cancel()
		key, name, err := b.ImportFromSpec(ctx, req.Spec, req.AlbumKey, q, pw, func(stage string, done, total int64) {
			s.jobs.update(job.ID, stage, done, total)
		})
		if name != "" {
			s.jobs.mu.Lock()
			if j, ok := s.jobs.byID[job.ID]; ok {
				j.Name = name
			}
			s.jobs.mu.Unlock()
		}
		s.jobs.finish(job.ID, key, err)
	}()

	writeJSON(w, http.StatusAccepted, map[string]any{"job": job})
}

func (s *Server) handleV1TGQueueUpload(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Limit      int    `json:"limit"`
		AlbumKey   string `json:"albumKey"`
		Quality    string `json:"quality"`
		Passphrase string `json:"passphrase"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 16*1024)).Decode(&req)

	b := s.currentBot()
	if b == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "Telegram bot is not running")
		return
	}
	q := gpmc.QualityOriginal
	switch req.Quality {
	case "saver":
		q = gpmc.QualitySaver
	case "quota":
		q = gpmc.QualityUseQuota
	}

	job := s.jobs.create("tg-queue", "Telegram Queue Drain", 0)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
		defer cancel()
		res, err := b.DrainQueueToGP(ctx, req.Limit, req.AlbumKey, q, req.Passphrase, func(msg string) {
			s.jobs.update(job.ID, msg, 0, 0)
		})
		s.jobs.finish(job.ID, fmt.Sprintf("%d uploaded", len(res)), err)
	}()
	writeJSON(w, http.StatusAccepted, map[string]any{"job": job})
}

func (s *Server) handleV1TGQueueClear(w http.ResponseWriter, _ *http.Request) {
	b := s.currentBot()
	if b == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "Telegram bot is not running")
		return
	}
	n := b.ClearQueueItems()
	writeJSON(w, http.StatusOK, map[string]any{"cleared": n})
}

func (s *Server) handleV1TGAutoUpload(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req) != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	b := s.currentBot()
	if b == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "Telegram bot is not running")
		return
	}
	b.SetAutoUpload(req.Enabled)
	writeJSON(w, http.StatusOK, map[string]any{"autoUpload": b.AutoUpload()})
}

func (s *Server) handleV1TGSend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MediaKey   string `json:"mediaKey"`
		Passphrase string `json:"passphrase"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 16*1024)).Decode(&req) != nil || strings.TrimSpace(req.MediaKey) == "" {
		writeJSONError(w, http.StatusBadRequest, "mediaKey is required")
		return
	}
	b := s.currentBot()
	if b == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "Telegram bot is not running")
		return
	}

	job := s.jobs.create("tg-send", "Send "+req.MediaKey+" to Telegram", 0)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		err := b.SendToOwner(ctx, req.MediaKey, req.Passphrase, func(msg string) {
			s.jobs.update(job.ID, msg, 0, 0)
		})
		s.jobs.finish(job.ID, req.MediaKey, err)
	}()
	writeJSON(w, http.StatusAccepted, map[string]any{"job": job})
}

func maskSecret(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if len(s) <= 8 {
		return "••••••••"
	}
	return s[:4] + "••••••••" + s[len(s)-4:]
}

func (s *Server) handleV1ConfigGet(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	fuseFn := s.hooks.GetFUSEStatus
	envPath := s.hooks.EnvFilePath
	s.mu.RUnlock()

	var fuseSt fusefs.Status
	if fuseFn != nil {
		fuseSt = fuseFn()
	}

	gp := s.client()
	email := ""
	if parsed, err := url.ParseQuery(gp.AuthData()); err == nil {
		email = parsed.Get("Email")
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"envPath":          envPath,
		"gpAuthConfigured": gp.AuthData() != "",
		"gpAuthMasked":     maskSecret(gp.AuthData()),
		"gpEmail":          email,
		"deviceProfile":    s.cfg.DeviceProfile,
		"encPassphraseSet": os.Getenv("GPIX_ENC_PASSPHRASE") != "",
		"tgBotTokenSet":    os.Getenv("TG_BOT_TOKEN") != "",
		"tgBotTokenMasked": maskSecret(os.Getenv("TG_BOT_TOKEN")),
		"tgApiId":          os.Getenv("TG_API_ID"),
		"tgApiHashSet":     os.Getenv("TG_API_HASH") != "",
		"tgOwnerId":        os.Getenv("TG_OWNER_ID"),
		"tgProxy":          os.Getenv("TG_PROXY"),
		"fuseMountPoint":   fuseSt.MountPoint,
		"fuseAllowOther":   fuseSt.AllowOther,
		"fuseMounted":      fuseSt.Mounted,
	})
}

func (s *Server) handleV1ConfigSave(w http.ResponseWriter, r *http.Request) {
	var req map[string]string
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	s.mu.RLock()
	updater := s.hooks.UpdateRuntime
	s.mu.RUnlock()
	if updater == nil {
		writeJSONError(w, http.StatusNotImplemented, "runtime config update not enabled")
		return
	}
	if err := updater(req); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleV1TestToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AuthData string `json:"authData"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 32*1024)).Decode(&req)
	auth := strings.TrimSpace(req.AuthData)
	if auth == "" {
		auth = s.client().AuthData()
	}
	testClient, err := gpmc.New(auth)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	if _, err := testClient.BearerToken(ctx); err != nil {
		writeJSONError(w, http.StatusUnauthorized, err.Error())
		return
	}
	quota, _ := testClient.GetStorageQuota(ctx)
	email := ""
	if parsed, err := url.ParseQuery(auth); err == nil {
		email = parsed.Get("Email")
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":    true,
		"email": email,
		"quota": quota,
	})
}

func (s *Server) handleV1FUSEMount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MountPoint string `json:"mountPoint"`
		AllowOther bool   `json:"allowOther"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 8192)).Decode(&req) != nil || strings.TrimSpace(req.MountPoint) == "" {
		writeJSONError(w, http.StatusBadRequest, "mountPoint is required")
		return
	}
	s.mu.RLock()
	remount := s.hooks.RemountFUSE
	s.mu.RUnlock()
	if remount == nil {
		writeJSONError(w, http.StatusNotImplemented, "FUSE manager unavailable")
		return
	}
	if err := remount(strings.TrimSpace(req.MountPoint), req.AllowOther); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "mountPoint": req.MountPoint})
}

func (s *Server) handleV1FUSEUnmount(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	unmount := s.hooks.UnmountFUSE
	s.mu.RUnlock()
	if unmount == nil {
		writeJSONError(w, http.StatusNotImplemented, "FUSE manager unavailable")
		return
	}
	if err := unmount(); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
