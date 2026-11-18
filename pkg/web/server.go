package web

import (
	"context"
	"crypto/rand"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"gpix/pkg/bridge"
	"gpix/pkg/fusefs"
	"gpix/pkg/gpmc"
	"gpix/pkg/vfs"
)

//go:embed templates/*.html templates/partials/*.html
var tmplFS embed.FS

//go:embed static
var staticFS embed.FS

type RuntimeHooks struct {
	VFS           *vfs.FS
	GetBot        func() *bridge.Bot
	GetFUSEStatus func() fusefs.Status
	RemountFUSE   func(mountPoint string, allowOther bool) error
	UnmountFUSE   func() error
	UpdateRuntime func(updates map[string]string) error
	EnvFilePath   string
	TrustLoopback bool
}

type Server struct {
	mu             sync.RWMutex
	cfg            Config
	gp             *gpmc.Client
	vfs            *vfs.FS
	hooks          RuntimeHooks
	jobs           *jobManager
	log            *slog.Logger
	httpSrv        *http.Server
	urlCache       *urlCache
	progressBus    *progressBus
	sessionSignKey []byte
	mediaSignKey   []byte
	tempSemaphore  chan struct{}
	pageTmpls      map[string]*template.Template
	extraMounts    map[string]http.Handler
}

func (s *Server) SetHooks(h RuntimeHooks) {
	s.mu.Lock()
	s.hooks = h
	if h.VFS != nil {
		s.vfs = h.VFS
	}
	s.mu.Unlock()
}

func (s *Server) SetClient(gp *gpmc.Client) {
	s.mu.Lock()
	s.gp = gp
	s.urlCache = newURLCache(gp)
	if s.vfs != nil {
		s.vfs.SetClient(gp)
	}
	s.mu.Unlock()
}

func (s *Server) client() *gpmc.Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gp
}

func (s *Server) vfsInstance() *vfs.FS {
	s.mu.RLock()
	v := s.vfs
	s.mu.RUnlock()
	return v
}

func (s *Server) Mount(prefix string, h http.Handler) {
	if s.extraMounts == nil {
		s.extraMounts = make(map[string]http.Handler)
	}
	s.extraMounts[prefix] = h
}

func New(cfg Config, gp *gpmc.Client, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.Default()
	}
	if len(cfg.SecretKey) < 32 {
		return nil, errors.New("web: SecretKey must be at least 32 bytes")
	}
	defaultVFS, err := vfs.New(gp, vfs.Config{
		TempDir:       cfg.TempDir,
		EncPassphrase: os.Getenv("GPIX_ENC_PASSPHRASE"),
	})
	if err != nil {
		return nil, fmt.Errorf("web: init vfs: %w", err)
	}
	s := &Server{
		cfg:            cfg,
		gp:             gp,
		vfs:            defaultVFS,
		jobs:           newJobManager(),
		log:            log,
		urlCache:       newURLCache(gp),
		progressBus:    newProgressBus(),
		sessionSignKey: deriveKey(cfg.SecretKey, "session"),
		mediaSignKey:   deriveKey(cfg.SecretKey, "media"),
		tempSemaphore:  make(chan struct{}, cfg.MaxConcurrentUploads),
	}
	if err := s.loadTemplates(); err != nil {
		return nil, fmt.Errorf("web: load templates: %w", err)
	}
	s.httpSrv = &http.Server{
		Addr:              cfg.Listen,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	return s, nil
}

func (s *Server) Run(ctx context.Context) error {
	s.httpSrv.Handler = s.routes()
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("web server listening", "addr", s.cfg.Listen)
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func LoadOrCreateSecret(path string) ([]byte, error) {
	if data, err := os.ReadFile(path); err == nil {
		if len(data) < 32 {
			return nil, fmt.Errorf("web: secret.key at %s is shorter than 32 bytes", path)
		}
		return data, nil
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, b, 0600); err != nil {
		return nil, err
	}
	return b, nil
}
