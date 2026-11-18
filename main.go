package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/crypto/bcrypt"

	"gpix/pkg/bridge"
	"gpix/pkg/fusefs"
	"gpix/pkg/gpmc"
	"gpix/pkg/s3api"
	"gpix/pkg/vfs"
	"gpix/pkg/web"
	"gpix/pkg/webdav"
)

func main() {
	mode := flag.String("mode", "all", "all | bot | web | fuse | cli | hashpw")
	cli := flag.Bool("cli", false, "shortcut for -mode cli")
	hashpw := flag.Bool("hashpw", false, "shortcut for -mode hashpw")
	fuseMode := flag.Bool("fuse", false, "shortcut for -mode fuse")

	envFlag := flag.String("env", ".env", "path to .env file (skipped if missing)")
	authFlag := flag.String("auth", "", "GP auth_data (defaults to $GP_AUTH_DATA)")
	profileFlag := flag.String("profile", "", "pixel-xl | pixel-5 (default: from web config or pixel-xl)")
	logLevel := flag.String("log", "info", "debug | info | warn | error")
	cfgFlag := flag.String("config", "gpix-web.conf", "path to web config file")
	secretFlag := flag.String("secret", "", "path to secret.key (default: alongside config)")

	mountFlag := flag.String("mount", "", "FUSE mountpoint directory (alias: -fuse-mount, or $GPIX_FUSE_MOUNT)")
	fuseMountFlag := flag.String("fuse-mount", "", "FUSE mountpoint directory")
	allowOtherFlag := flag.Bool("fuse-allow-other", false, "pass allow_other to FUSE mount")
	fuseROFlag := flag.Bool("fuse-ro", false, "mount FUSE filesystem read-only")
	fuseDebugFlag := flag.Bool("fuse-debug", false, "enable FUSE protocol debug logging")

	tempDirFlag := flag.String("temp-dir", "", "staging & decrypt cache directory (defaults to $GPIX_TEMP_DIR)")
	metaFileFlag := flag.String("meta-file", "", "path to VFS POSIX metadata store (vfs-meta.json)")
	trustLoopback := flag.Bool("trust-loopback", false, "trust loopback (127.0.0.1/::1) requests behind a local reverse proxy")

	qualityFlag := flag.String("quality", "original", "[cli/fuse] original | saver | quota")
	conc := flag.Int("concurrency", 1, "[cli] parallel uploads")
	recursive := flag.Bool("recursive", false, "[cli] descend into directories")
	force := flag.Bool("force", false, "[cli] skip dedup")
	deleteAfter := flag.Bool("delete-after", false, "[cli] delete local file after successful upload")

	flag.Parse()

	switch {
	case *cli:
		*mode = "cli"
	case *hashpw:
		*mode = "hashpw"
	case *fuseMode:
		*mode = "fuse"
	}

	if err := loadDotEnv(*envFlag); err != nil {
		fmt.Fprintln(os.Stderr, "warn: load env:", err)
	}

	if *tempDirFlag != "" {
		_ = os.Setenv("GPIX_TEMP_DIR", *tempDirFlag)
		if os.Getenv("TG_TEMP_DIR") == "" {
			_ = os.Setenv("TG_TEMP_DIR", *tempDirFlag)
		}
	}

	log := newLogger(*logLevel)

	switch *mode {
	case "hashpw":
		runHashpw()
		return
	case "cli":
		runCLI(*authFlag, *profileFlag, *qualityFlag, *conc, *recursive, *force, *deleteAfter)
		return
	}

	auth := coalesce(*authFlag, os.Getenv("GP_AUTH_DATA"))
	mountPoint := coalesce(*mountFlag, *fuseMountFlag, os.Getenv("GPIX_FUSE_MOUNT"))
	allowOther := *allowOtherFlag || os.Getenv("GPIX_FUSE_ALLOW_OTHER") == "1" || os.Getenv("GPIX_FUSE_ALLOW_OTHER") == "true"

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	switch *mode {
	case "fuse":
		if auth == "" {
			die("missing GP_AUTH_DATA (in .env, env var, or -auth flag)")
		}
		if mountPoint == "" {
			die("missing FUSE mountpoint: pass -mount /path/to/dir (or set GPIX_FUSE_MOUNT)")
		}
		runFUSE(ctx, log, auth, *cfgFlag, *profileFlag, *qualityFlag, mountPoint, allowOther, *fuseROFlag, *fuseDebugFlag, *tempDirFlag, *metaFileFlag)
	case "bot":
		if auth == "" {
			die("missing GP_AUTH_DATA (in .env, env var, or -auth flag)")
		}
		runBot(ctx, log, auth, *profileFlag)
	case "web":
		if auth == "" && !*trustLoopback {
			die("missing GP_AUTH_DATA (in .env, env var, or -auth flag)")
		}
		runUnified(ctx, log, false, auth, *envFlag, *cfgFlag, *secretFlag, *profileFlag, *qualityFlag, mountPoint, allowOther, *fuseROFlag, *fuseDebugFlag, *tempDirFlag, *metaFileFlag, *trustLoopback)
	case "all":
		if auth == "" && !*trustLoopback {
			die("missing GP_AUTH_DATA (in .env, env var, or -auth flag)")
		}
		runUnified(ctx, log, true, auth, *envFlag, *cfgFlag, *secretFlag, *profileFlag, *qualityFlag, mountPoint, allowOther, *fuseROFlag, *fuseDebugFlag, *tempDirFlag, *metaFileFlag, *trustLoopback)
	default:
		die("unknown mode: " + *mode + " (want all|bot|web|fuse|cli|hashpw)")
	}
}

func runFUSE(ctx context.Context, log *slog.Logger, auth, cfgPath, profileFlag, qualityFlag, mountPoint string, allowOther, readOnly, debug bool, tempDir, metaFile string) {
	q, err := parseQuality(qualityFlag)
	if err != nil {
		die(err.Error())
	}
	profile, err := parseProfile(coalesce(profileFlag, "pixel-xl"))
	if err != nil {
		die(err.Error())
	}
	gp, err := gpmc.New(auth, gpmc.WithDeviceProfile(profile), gpmc.WithAlbumStore(albumStorePath(cfgPath)))
	if err != nil {
		die("gpmc.New: " + err.Error())
	}
	if metaFile == "" {
		metaFile = defaultMetaPath(cfgPath)
	}
	v, err := vfs.New(gp, vfs.Config{
		TempDir:       coalesce(tempDir, os.Getenv("GPIX_TEMP_DIR")),
		MetaFile:      metaFile,
		EncPassphrase: os.Getenv("GPIX_ENC_PASSPHRASE"),
		Quality:       q,
	})
	if err != nil {
		die("vfs.New: " + err.Error())
	}

	h, err := fusefs.Mount(ctx, v, fusefs.Options{
		MountPoint: mountPoint,
		AllowOther: allowOther,
		ReadOnly:   readOnly,
		Debug:      debug,
		Logger:     log.With("service", "fuse"),
	})
	if err != nil {
		die("fuse mount failed: " + err.Error())
	}
	log.Info("gpix FUSE mounted", "mount", mountPoint, "allow_other", allowOther, "read_only", readOnly)
	h.Wait()
	log.Info("gpix FUSE unmounted", "mount", mountPoint)
}

type runtimeController struct {
	ctx        context.Context
	log        *slog.Logger
	envPath    string
	cfgPath    string
	enableBot  bool
	fuseRO     bool
	fuseDebug  bool
	webSrv     *web.Server
	sharedVFS  *vfs.FS

	mu             sync.Mutex
	gp             *gpmc.Client
	bot            *bridge.Bot
	botCancel      context.CancelFunc
	fuseHandle     fusefs.Handle
	fuseCancel     context.CancelFunc
	fuseMountPoint string
	fuseAllowOther bool
	fuseErr        string
}

func (rc *runtimeController) getBot() *bridge.Bot {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.bot
}

func (rc *runtimeController) fuseStatus() fusefs.Status {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return fusefs.Status{
		Supported:  fusefs.Supported,
		Mounted:    rc.fuseHandle != nil,
		MountPoint: rc.fuseMountPoint,
		AllowOther: rc.fuseAllowOther,
		Error:      rc.fuseErr,
	}
}

func (rc *runtimeController) mountFUSE(mountPoint string, allowOther bool) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.mountFUSELocked(mountPoint, allowOther)
}

func (rc *runtimeController) mountFUSELocked(mountPoint string, allowOther bool) error {
	if rc.fuseHandle != nil {
		_ = rc.fuseHandle.Unmount()
		if rc.fuseCancel != nil {
			rc.fuseCancel()
		}
		rc.fuseHandle = nil
	}
	rc.fuseMountPoint = mountPoint
	rc.fuseAllowOther = allowOther
	if mountPoint == "" {
		rc.fuseErr = ""
		return nil
	}
	fCtx, cancel := context.WithCancel(rc.ctx)
	h, err := fusefs.Mount(fCtx, rc.sharedVFS, fusefs.Options{
		MountPoint: mountPoint,
		AllowOther: allowOther,
		ReadOnly:   rc.fuseRO,
		Debug:      rc.fuseDebug,
		Logger:     rc.log.With("service", "fuse"),
	})
	if err != nil {
		cancel()
		rc.fuseErr = err.Error()
		return err
	}
	rc.fuseHandle = h
	rc.fuseCancel = cancel
	rc.fuseErr = ""
	rc.log.Info("gpix FUSE mounted", "mount", mountPoint, "allow_other", allowOther)
	return nil
}

func (rc *runtimeController) unmountFUSE() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.fuseHandle == nil {
		return nil
	}
	err := rc.fuseHandle.Unmount()
	if rc.fuseCancel != nil {
		rc.fuseCancel()
	}
	rc.fuseHandle = nil
	rc.fuseCancel = nil
	rc.log.Info("gpix FUSE unmounted", "mount", rc.fuseMountPoint)
	return err
}

func (rc *runtimeController) startOrRestartBotAsync() {
	rc.mu.Lock()
	if rc.botCancel != nil {
		rc.botCancel()
		rc.botCancel = nil
	}
	oldBot := rc.bot
	rc.bot = nil
	gp := rc.gp
	rc.mu.Unlock()

	if oldBot != nil {
		_ = oldBot.Stop()
	}

	go func() {
		cfg, err := bridge.LoadConfigFromEnv()
		if err != nil {
			rc.log.Info("telegram bot skipped", "reason", err.Error())
			return
		}
		if tmp := os.Getenv("GPIX_TEMP_DIR"); tmp != "" && os.Getenv("TG_TEMP_DIR") == "" {
			cfg.TempDir = tmp
		}
		b, err := bridge.New(cfg, gp, rc.log.With("service", "bot"))
		if err != nil {
			rc.log.Warn("telegram bot init failed", "err", err)
			return
		}
		if os.Getenv("GPIX_TG_AUTO_UPLOAD") == "1" || os.Getenv("GPIX_TG_AUTO_UPLOAD") == "true" {
			b.SetAutoUpload(true)
		}
		bCtx, cancel := context.WithCancel(rc.ctx)
		rc.mu.Lock()
		rc.bot = b
		rc.botCancel = cancel
		rc.mu.Unlock()

		rc.log.Info("gpix bot started", "owner", cfg.OwnerID, "temp_dir", cfg.TempDir)
		if err := b.Start(bCtx); err != nil && bCtx.Err() == nil {
			rc.log.Warn("telegram bot stopped", "err", err)
		}
	}()
}

func (rc *runtimeController) updateRuntime(updates map[string]string) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	envToSave := map[string]string{}
	needClientReload := false
	needBotReload := false
	needRemount := false
	newMount := rc.fuseMountPoint
	newAllow := rc.fuseAllowOther

	for k, v := range updates {
		v = strings.TrimSpace(v)
		switch k {
		case "GP_AUTH_DATA":
			if v != "" && !strings.Contains(v, "••••") {
				_ = os.Setenv("GP_AUTH_DATA", v)
				envToSave["GP_AUTH_DATA"] = v
				needClientReload = true
			}
		case "GPIX_ENC_PASSPHRASE":
			if v != "••••••••" {
				_ = os.Setenv("GPIX_ENC_PASSPHRASE", v)
				envToSave["GPIX_ENC_PASSPHRASE"] = v
				rc.sharedVFS.UpdatePassphrase(v)
			}
		case "TG_BOT_TOKEN":
			if v != "" && !strings.Contains(v, "••••") {
				_ = os.Setenv("TG_BOT_TOKEN", v)
				envToSave["TG_BOT_TOKEN"] = v
				needBotReload = true
			}
		case "TG_API_ID", "TG_OWNER_ID", "TG_PROXY":
			_ = os.Setenv(k, v)
			envToSave[k] = v
			needBotReload = true
		case "TG_API_HASH":
			if v != "" && !strings.Contains(v, "••••") {
				_ = os.Setenv("TG_API_HASH", v)
				envToSave["TG_API_HASH"] = v
				needBotReload = true
			}
		case "GPIX_FUSE_MOUNT":
			if v != rc.fuseMountPoint {
				_ = os.Setenv("GPIX_FUSE_MOUNT", v)
				envToSave["GPIX_FUSE_MOUNT"] = v
				newMount = v
				needRemount = true
			}
		case "GPIX_FUSE_ALLOW_OTHER":
			allow := v == "1" || v == "true" || v == "on"
			if allow != rc.fuseAllowOther {
				_ = os.Setenv("GPIX_FUSE_ALLOW_OTHER", v)
				envToSave["GPIX_FUSE_ALLOW_OTHER"] = v
				newAllow = allow
				needRemount = true
			}
		}
	}

	if len(envToSave) > 0 && rc.envPath != "" {
		if err := saveDotEnvUpdates(rc.envPath, envToSave); err != nil {
			return fmt.Errorf("save .env: %w", err)
		}
	}

	if needClientReload {
		prof := rc.gp.DeviceProfile()
		newGP, err := gpmc.New(os.Getenv("GP_AUTH_DATA"), gpmc.WithDeviceProfile(prof), gpmc.WithAlbumStore(albumStorePath(rc.cfgPath)))
		if err != nil {
			return err
		}
		rc.gp = newGP
		rc.webSrv.SetClient(newGP)
		if rc.bot != nil {
			rc.bot.SetGPClient(newGP)
		}
	}

	if needBotReload {
		go rc.startOrRestartBotAsync()
	}

	if needRemount {
		if newMount == "" {
			if rc.fuseHandle != nil {
				_ = rc.fuseHandle.Unmount()
				rc.fuseHandle = nil
			}
			rc.fuseMountPoint = ""
		} else {
			if err := rc.mountFUSELocked(newMount, newAllow); err != nil {
				return err
			}
		}
	}

	return nil
}

func runUnified(ctx context.Context, log *slog.Logger, withBot bool, auth, envPath, cfgPath, secretPath, profileFlag, qualityFlag, mountPoint string, allowOther, fuseRO, fuseDebug bool, tempDir, metaFile string, trustLoopback bool) {
	cfg, err := web.LoadConfig(cfgPath)
	if err != nil {
		die("web config: " + err.Error())
	}
	if tempDir != "" {
		cfg.TempDir = tempDir
	}

	if secretPath == "" {
		secretPath = filepath.Join(filepath.Dir(cfgPath), "secret.key")
	}
	secret, err := web.LoadOrCreateSecret(secretPath)
	if err != nil {
		die("secret: " + err.Error())
	}
	cfg.SecretKey = secret

	profileName := coalesce(profileFlag, cfg.DeviceProfile, "pixel-xl")
	profile, err := parseProfile(profileName)
	if err != nil {
		die(err.Error())
	}
	q, err := parseQuality(qualityFlag)
	if err != nil {
		die(err.Error())
	}

	if auth == "" {
		auth = "placeholder=unconfigured"
	}
	gp, err := gpmc.New(auth, gpmc.WithDeviceProfile(profile), gpmc.WithAlbumStore(albumStorePath(cfgPath)))
	if err != nil {
		die("gpmc.New: " + err.Error())
	}

	if metaFile == "" {
		metaFile = defaultMetaPath(cfgPath)
	}
	sharedVFS, err := vfs.New(gp, vfs.Config{
		TempDir:       cfg.TempDir,
		MetaFile:      metaFile,
		EncPassphrase: os.Getenv("GPIX_ENC_PASSPHRASE"),
		Quality:       q,
	})
	if err != nil {
		die("vfs.New: " + err.Error())
	}

	srv, err := web.New(cfg, gp, log.With("service", "web"))
	if err != nil {
		die("web.New: " + err.Error())
	}

	rc := &runtimeController{
		ctx:       ctx,
		log:       log,
		envPath:   envPath,
		cfgPath:   cfgPath,
		enableBot: withBot,
		fuseRO:    fuseRO,
		fuseDebug: fuseDebug,
		webSrv:    srv,
		sharedVFS: sharedVFS,
		gp:        gp,
	}

	srv.SetHooks(web.RuntimeHooks{
		VFS:           sharedVFS,
		GetBot:        rc.getBot,
		GetFUSEStatus: rc.fuseStatus,
		RemountFUSE:   rc.mountFUSE,
		UnmountFUSE:   rc.unmountFUSE,
		UpdateRuntime: rc.updateRuntime,
		EnvFilePath:   envPath,
		TrustLoopback: trustLoopback,
	})

	mountWebDAV(srv, sharedVFS, log)
	mountS3(srv, gp, log)

	if mountPoint != "" {
		if err := rc.mountFUSE(mountPoint, allowOther); err != nil {
			log.Warn("initial FUSE mount failed", "mount", mountPoint, "err", err)
		}
	}

	if withBot {
		rc.startOrRestartBotAsync()
	}

	log.Info("gpix started",
		"listen", cfg.Listen,
		"profile", profileName,
		"fuse_mount", mountPoint,
		"trust_loopback", trustLoopback,
	)
	err = srv.Run(ctx)
	_ = rc.unmountFUSE()
	rc.mu.Lock()
	if rc.bot != nil {
		_ = rc.bot.Stop()
	}
	rc.mu.Unlock()
	if err != nil {
		log.Error("gpix stopped with error", "err", err)
		os.Exit(1)
	}
}

func runBot(ctx context.Context, log *slog.Logger, auth, profileFlag string) {
	if err := startBot(ctx, log, auth, profileFlag); err != nil {
		log.Error("bot stopped", "err", err)
		os.Exit(1)
	}
}

func defaultMetaPath(cfgPath string) string {
	dir := filepath.Dir(cfgPath)
	if dir == "" || dir == "." {
		if home, err := os.UserHomeDir(); err == nil {
			dir = home
		} else {
			dir = "."
		}
	}
	return filepath.Join(dir, "gpix-vfs-meta.json")
}

func albumStorePath(cfgPath string) string {
	dir := filepath.Dir(cfgPath)
	if dir == "" || dir == "." {
		if home, err := os.UserHomeDir(); err == nil {
			dir = home
		} else {
			dir = "."
		}
	}
	return filepath.Join(dir, "gpix-albums.json")
}

func cfgPathForBot() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, "gpix-bot")
	}
	return "gpix-bot"
}

func startBot(ctx context.Context, log *slog.Logger, auth, profileFlag string) error {
	cfg, err := bridge.LoadConfigFromEnv()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	profile, err := parseProfile(coalesce(profileFlag, "pixel-xl"))
	if err != nil {
		return err
	}

	gp, err := gpmc.New(auth, gpmc.WithDeviceProfile(profile), gpmc.WithAlbumStore(albumStorePath(cfgPathForBot())))
	if err != nil {
		return fmt.Errorf("gpmc.New: %w", err)
	}

	bot, err := bridge.New(cfg, gp, log)
	if err != nil {
		return fmt.Errorf("bridge.New: %w", err)
	}

	log.Info("gpix bot started",
		"owner", cfg.OwnerID,
		"temp_dir", cfg.TempDir,
		"max_concurrent", cfg.MaxConcurrent,
	)
	return bot.Start(ctx)
}

func mountWebDAV(srv *web.Server, sharedVFS *vfs.FS, log *slog.Logger) {
	cfg, err := webdav.LoadFromEnv()
	if err != nil {
		log.Debug("webdav disabled", "reason", err.Error())
		return
	}
	davVFS, err := vfs.New(sharedVFS.Client(), vfs.Config{
		EncPassphrase:    coalesce(cfg.EncPassphrase, sharedVFS.Passphrase()),
		StrictWebDAVRoot: true,
	})
	if err != nil {
		log.Warn("webdav vfs init failed", "err", err)
		return
	}
	dav, err := webdav.NewServerWithVFS(cfg, davVFS, log.With("service", "webdav"))
	if err != nil {
		log.Warn("webdav init failed", "err", err)
		return
	}
	srv.Mount("/dav/", dav.Handler())
	log.Info("webdav mounted", "path", "/dav/")
}

func mountS3(srv *web.Server, gp *gpmc.Client, log *slog.Logger) {
	cfg, err := s3api.LoadFromEnv()
	if err != nil {
		log.Debug("s3 disabled", "reason", err.Error())
		return
	}
	s3srv, err := s3api.NewServer(cfg, gp, log.With("service", "s3"))
	if err != nil {
		log.Warn("s3 init failed", "err", err)
		return
	}
	srv.Mount("/s3/", http.StripPrefix("/s3", s3srv.Handler()))
	log.Info("s3 mounted", "path", "/s3/", "bucket", cfg.Bucket)
}

func runCLI(authFlag, profileFlag, qualityFlag string, conc int, recursive, force, deleteAfter bool) {
	auth := authFlag
	if auth == "" {
		auth = os.Getenv("GP_AUTH_DATA")
	}
	if auth == "" {
		die("missing GP auth: pass -auth or set GP_AUTH_DATA")
	}
	if flag.NArg() == 0 {
		die("missing positional path argument")
	}

	q, err := parseQuality(qualityFlag)
	if err != nil {
		die(err.Error())
	}
	profile, err := parseProfile(coalesce(profileFlag, "pixel-xl"))
	if err != nil {
		die(err.Error())
	}

	client, err := gpmc.New(auth, gpmc.WithDeviceProfile(profile))
	if err != nil {
		die(err.Error())
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	opts := gpmc.UploadOpts{
		Quality:     q,
		Force:       force,
		Concurrency: conc,
		Recursive:   recursive,
		DeleteAfter: deleteAfter,
	}

	results, err := client.UploadFiles(ctx, flag.Args(), opts, func(ev gpmc.UploadEvent) {
		if ev.Err != nil {
			fmt.Fprintf(os.Stderr, "[%s] %s: %v\n", ev.Stage, ev.Path, ev.Err)
			return
		}
		fmt.Fprintf(os.Stderr, "[%s] %s\n", ev.Stage, ev.Path)
	})
	if err != nil {
		die(err.Error())
	}

	var failures int
	for _, r := range results {
		if r.Err != nil {
			fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", r.Path, r.Err)
			failures++
			continue
		}
		marker := "OK"
		if r.Skipped {
			marker = "SKIP"
		}
		fmt.Printf("%s\t%s\t%s\n", marker, r.Path, r.MediaKey)
	}
	if failures > 0 {
		os.Exit(2)
	}
}

func runHashpw() {
	fmt.Fprint(os.Stderr, "password: ")
	r := bufio.NewReader(os.Stdin)
	pw, _ := r.ReadString('\n')
	pw = strings.TrimRight(pw, "\r\n")
	if pw == "" {
		die("empty password")
	}
	h, err := bcrypt.GenerateFromPassword([]byte(pw), 12)
	if err != nil {
		die(err.Error())
	}
	fmt.Println(string(h))
}

func parseQuality(s string) (gpmc.Quality, error) {
	switch s {
	case "original":
		return gpmc.QualityOriginal, nil
	case "saver":
		return gpmc.QualitySaver, nil
	case "quota":
		return gpmc.QualityUseQuota, nil
	}
	return 0, fmt.Errorf("unknown quality %q (want original|saver|quota)", s)
}

func parseProfile(s string) (gpmc.DeviceProfile, error) {
	switch s {
	case "", "pixel-xl":
		return gpmc.DefaultPixelXL(), nil
	case "pixel-5":
		return gpmc.DefaultPixel5(), nil
	}
	return gpmc.DeviceProfile{}, fmt.Errorf("unknown profile %q", s)
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func coalesce(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, "error:", msg)
	os.Exit(1)
}

func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		if len(val) >= 2 && (val[0] == '"' && val[len(val)-1] == '"' || val[0] == '\'' && val[len(val)-1] == '\'') {
			val = val[1 : len(val)-1]
		}
		if _, set := os.LookupEnv(key); set {
			continue
		}
		_ = os.Setenv(key, val)
	}
	return sc.Err()
}

func saveDotEnvUpdates(path string, updates map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil && filepath.Dir(path) != "." {
		return err
	}
	var lines []string
	seen := map[string]bool{}
	if existing, err := os.ReadFile(path); err == nil {
		lines = strings.Split(strings.ReplaceAll(string(existing), "\r\n", "\n"), "\n")
	}
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		eq := strings.IndexByte(trimmed, '=')
		if eq <= 0 {
			continue
		}
		k := strings.TrimSpace(trimmed[:eq])
		if newVal, ok := updates[k]; ok {
			lines[i] = formatEnvKV(k, newVal)
			seen[k] = true
		}
	}
	var extraKeys []string
	for k := range updates {
		if !seen[k] {
			extraKeys = append(extraKeys, k)
		}
	}
	sort.Strings(extraKeys)
	for _, k := range extraKeys {
		lines = append(lines, formatEnvKV(k, updates[k]))
	}
	out := strings.TrimRight(strings.Join(lines, "\n"), "\n") + "\n"
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(out), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func formatEnvKV(k, v string) string {
	if strings.ContainsAny(v, " \t#$'\"") {
		return fmt.Sprintf("%s='%s'", k, strings.ReplaceAll(v, "'", ""))
	}
	return k + "=" + v
}

var _ = errors.New
