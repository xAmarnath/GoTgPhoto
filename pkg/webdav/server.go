package webdav

import (
	"log/slog"
	"net/http"
	"strings"

	"golang.org/x/net/webdav"

	"gpix/pkg/gpmc"
	"gpix/pkg/vfs"
)

const Prefix = "/dav"

type Server struct {
	cfg     Config
	gp      *gpmc.Client
	vfs     *vfs.FS
	log     *slog.Logger
	handler *webdav.Handler
}

func NewServer(cfg Config, gp *gpmc.Client, log *slog.Logger) (*Server, error) {
	v, err := vfs.New(gp, vfs.Config{
		EncPassphrase:    cfg.EncPassphrase,
		StrictWebDAVRoot: true,
	})
	if err != nil {
		return nil, err
	}
	return NewServerWithVFS(cfg, v, log)
}

func NewServerWithVFS(cfg Config, v *vfs.FS, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{cfg: cfg, gp: v.Client(), vfs: v, log: log}
	s.handler = &webdav.Handler{
		Prefix:     Prefix,
		FileSystem: newFS(v, cfg),
		LockSystem: webdav.NewMemLS(),
		Logger: func(r *http.Request, err error) {
			if err != nil {
				log.Debug("webdav", "method", r.Method, "path", r.URL.Path, "err", err)
			}
		},
	}
	return s, nil
}

func (s *Server) Handler() http.Handler {
	return s.basicAuth(http.HandlerFunc(s.serve))
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodOptions:
		w.Header().Set("Allow", "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, PROPFIND, LOCK, UNLOCK")
		w.Header().Set("DAV", "1, 2")
		w.Header().Set("MS-Author-Via", "DAV")
		w.WriteHeader(http.StatusOK)
		return
	case "MKCOL":
		rel := strings.TrimPrefix(r.URL.Path, Prefix)
		clean := vfs.CleanPath(rel)
		parts := strings.Split(strings.TrimPrefix(clean, "/"), "/")
		if !(len(parts) == 2 && parts[0] == "albums" && parts[1] != "") {
			http.Error(w, "collections are read-only", http.StatusForbidden)
			return
		}
	}
	s.handler.ServeHTTP(w, r)
}
