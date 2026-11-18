package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"gpix/pkg/disguise"
	"gpix/pkg/gpmc"
)

func (s *Server) handleThumb(w http.ResponseWriter, r *http.Request) {
	s.serveThumbByKey(w, r, r.PathValue("key"))
}

func (s *Server) serveThumbByKey(w http.ResponseWriter, r *http.Request, key string) {
	if key == "" {
		http.NotFound(w, r)
		return
	}
	size := 256
	if v := r.URL.Query().Get("size"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			switch n {
			case 64, 128, 256, 512:
				size = n
			}
		}
	}

	ctx, cancel := withTimeout(r.Context(), 20*time.Second)
	defer cancel()

	gp := s.client()
	tok, err := gp.BearerToken(ctx)
	if err != nil {
		http.Error(w, "bearer: "+err.Error(), 500)
		return
	}
	url := gpmc.ThumbnailURL(key, size)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := gp.HTTPClient().Do(req)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		http.Error(w, fmt.Sprintf("thumb upstream %d", resp.StatusCode), resp.StatusCode)
		return
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.Header().Set("Cache-Control", "private, max-age=86400, immutable")
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	key, err := s.verifyMedia(r.PathValue("token"))
	if err != nil {
		http.Error(w, "bad token", http.StatusForbidden)
		return
	}
	s.serveStreamByKey(w, r, key)
}

func (s *Server) serveStreamByKey(w http.ResponseWriter, r *http.Request, key string) {
	mode := r.URL.Query().Get("mode")
	levelQ := r.URL.Query().Get("level")
	if mode == "hls" || levelQ != "" {
		ctx, cancel := withTimeout(r.Context(), 30*time.Second)
		defer cancel()
		gp := s.client()
		manifest, err := gp.GetStreamManifest(ctx, key, "hls")
		if err == nil {
			if levelQ != "" {
				idx, perr := strconv.Atoi(levelQ)
				if perr == nil {
					variants := ParseMasterPlaylist(manifest)
					for _, v := range variants {
						if v.Index == idx {
							http.Redirect(w, r, v.PlaylistURL, http.StatusFound)
							return
						}
					}
				}
				http.Error(w, "unknown level", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.Header().Set("Cache-Control", "private, max-age=300")
			_, _ = io.WriteString(w, manifest)
			return
		}
	}
	s.serveRangeMedia(w, r, key, false)
}

func (s *Server) handleRaw(w http.ResponseWriter, r *http.Request) {
	key, err := s.verifyMedia(r.PathValue("token"))
	if err != nil {
		http.Error(w, "bad token", http.StatusForbidden)
		return
	}
	s.serveRangeMedia(w, r, key, true)
}

// serveRangeMedia serves any mediaKey (native, disguised V1, or encrypted V2) with full RFC 9110
// HTTP Range (206 Partial Content) support via pkg/vfs's block-cached SeekableReader.
func (s *Server) serveRangeMedia(w http.ResponseWriter, r *http.Request, mediaKey string, attachment bool) {
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	ctx := r.Context()
	gp := s.client()
	v := s.vfsInstance()

	it, found, _ := gp.GetItem(ctx, mediaKey)
	if !found {
		it = gpmc.MediaItem{MediaKey: mediaKey}
	}

	pw := r.URL.Query().Get("passphrase")
	sr, err := v.OpenReader(ctx, it, pw)
	if err != nil {
		if errors.Is(err, disguise.ErrEncrypted) {
			writeJSONError(w, http.StatusLocked, "file is encrypted; passphrase required")
			return
		}
		if errors.Is(err, disguise.ErrWrongPassphrase) {
			writeJSONError(w, http.StatusForbidden, "wrong decryption passphrase")
			return
		}
		http.Error(w, "open media: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer sr.Close()

	name := sr.Name()
	if name == "" {
		name = mediaKey
	}
	contentType := disguiseMIME(name)
	if contentType == "application/octet-stream" && !sr.IsDisguised() {
		switch it.Kind {
		case gpmc.KindVideo:
			contentType = "video/mp4"
		case gpmc.KindPhoto:
			contentType = "image/jpeg"
		}
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Accept-Ranges", "bytes")

	dispType := "inline"
	if attachment {
		dispType = "attachment"
	}
	if disp := mime.FormatMediaType(dispType, map[string]string{"filename": name}); disp != "" {
		w.Header().Set("Content-Disposition", disp)
	}

	if sr.Size() > 0 {
		http.ServeContent(w, r, name, sr.ModTime(), sr)
		return
	}

	s.proxyDownloadFallback(w, r, mediaKey, attachment)
}

func (s *Server) proxyDownloadFallback(w http.ResponseWriter, r *http.Request, mediaKey string, attachment bool) {
	ctx, cancel := withTimeout(r.Context(), 15*time.Minute)
	defer cancel()

	dlURL, err := s.urlCache.Get(ctx, mediaKey)
	if err != nil {
		http.Error(w, "resolve: "+err.Error(), http.StatusBadGateway)
		return
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, dlURL, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if rng := r.Header.Get("Range"); rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := s.client().HTTPClient().Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for _, h := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges"} {
		if hv := resp.Header.Get(h); hv != "" {
			w.Header().Set(h, hv)
		}
	}
	if attachment && w.Header().Get("Content-Disposition") == "" {
		w.Header().Set("Content-Disposition", "attachment")
	}
	w.WriteHeader(resp.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = io.Copy(w, resp.Body)
	}
}

func disguiseMIME(filename string) string {
	if filename == "" {
		return "application/octet-stream"
	}
	ext := filepath.Ext(filename)
	if m := mime.TypeByExtension(ext); m != "" {
		return m
	}
	return disguise.MIMEForFilename(filename)
}

func (s *Server) doProxyGet(ctx context.Context, url, mediaKey string, _ bool) (*http.Response, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false, err
	}
	resp, err := s.client().HTTPClient().Do(req)
	return resp, false, err
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}
