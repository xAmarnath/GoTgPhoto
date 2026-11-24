package vfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gpix/pkg/disguise"
	"gpix/pkg/gpmc"
	"gpix/pkg/gpmc/albumstore"
)

const maxPages = 50

type Config struct {
	TempDir          string
	MetaFile         string
	EncPassphrase    string
	Quality          gpmc.Quality
	MaxCacheBlocks   int
	StrictWebDAVRoot bool
}

type QuotaInfo struct {
	TotalBytes      uint64 `json:"totalBytes"`
	UsedBytes       uint64 `json:"usedBytes"`
	AvailBytes      uint64 `json:"availBytes"`
	PhotosUsedBytes uint64 `json:"photosUsedBytes"`
	Unlimited       bool   `json:"unlimited"`
	BlockSize       uint32 `json:"blockSize"`
	TotalBlocks     uint64 `json:"totalBlocks"`
	FreeBlocks      uint64 `json:"freeBlocks"`
	AvailBlocks     uint64 `json:"availBlocks"`
	UpdatedAt       int64  `json:"updatedAt"`
}

type NodeInfo struct {
	FileName      string
	FileSize      int64
	FileMode      fs.FileMode
	Mtime         time.Time
	Atime         time.Time
	IsDirFlag     bool
	Ino           uint64
	Nlink         uint32
	UID           uint32
	GID           uint32
	SymlinkTarget string
	MediaKey      string
}

func (n *NodeInfo) Name() string       { return n.FileName }
func (n *NodeInfo) Size() int64        { return n.FileSize }
func (n *NodeInfo) Mode() fs.FileMode  { return n.FileMode }
func (n *NodeInfo) ModTime() time.Time { return n.Mtime }
func (n *NodeInfo) IsDir() bool        { return n.IsDirFlag }
func (n *NodeInfo) Sys() any           { return n }

type FS struct {
	gp       *gpmc.Client
	cfgMu    sync.RWMutex
	cfg      Config
	meta     *MetaStore
	urls     *downloadURLCache
	chunks   *rangeChunkCache
	decCache *diskDecryptCache

	quotaMu         sync.Mutex
	quota           QuotaInfo
	quotaAt         time.Time
	quotaRefreshing bool

	activeStaged int64
}

func New(gp *gpmc.Client, cfg Config) (*FS, error) {
	if cfg.TempDir == "" {
		if envTmp := os.Getenv("GPIX_TEMP_DIR"); envTmp != "" {
			cfg.TempDir = envTmp
		} else {
			cfg.TempDir = os.TempDir()
		}
	}
	_ = os.MkdirAll(cfg.TempDir, 0o755)

	meta, err := OpenMetaStore(cfg.MetaFile)
	if err != nil {
		return nil, fmt.Errorf("vfs: open meta store: %w", err)
	}
	urls := newDownloadURLCache(gp)
	chunks := newRangeChunkCache(urls, cfg.MaxCacheBlocks)
	dec := newDiskDecryptCache(cfg.TempDir)
	if cfg.TempDir != "" {
		chunks.spoolDir = filepath.Join(cfg.TempDir, "gpix-spool")
	}
	_ = os.RemoveAll(chunks.spoolDirOrDefault()) // leftovers from a previous run

	return &FS{
		gp:       gp,
		cfg:      cfg,
		meta:     meta,
		urls:     urls,
		chunks:   chunks,
		decCache: dec,
	}, nil
}

func (f *FS) Client() *gpmc.Client { return f.gp }
func (f *FS) Meta() *MetaStore     { return f.meta }

func (f *FS) SetClient(gp *gpmc.Client) {
	f.cfgMu.Lock()
	f.gp = gp
	f.urls.gp = gp
	f.cfgMu.Unlock()
}

func (f *FS) UpdatePassphrase(pw string) {
	f.cfgMu.Lock()
	f.cfg.EncPassphrase = pw
	f.cfgMu.Unlock()
}

func (f *FS) Passphrase() string {
	f.cfgMu.RLock()
	defer f.cfgMu.RUnlock()
	return f.cfg.EncPassphrase
}

func (f *FS) CacheStats() (cachedBlocks int, activeStaged int64) {
	return f.chunks.Len(), atomic.LoadInt64(&f.activeStaged)
}

func (f *FS) InvalidateMedia(mediaKey string) {
	f.urls.Invalidate(mediaKey)
	f.chunks.EvictMedia(mediaKey)
	f.decCache.Delete(mediaKey)
}

func (f *FS) stagingDir() string {
	f.cfgMu.RLock()
	base := f.cfg.TempDir
	f.cfgMu.RUnlock()
	return filepath.Join(base, "gpix-staging")
}

func CleanPath(name string) string {
	name = strings.ReplaceAll(name, "\\", "/")
	name = strings.TrimSuffix(path.Clean("/"+strings.TrimSpace(name)), "/")
	if name == "" {
		return "/"
	}
	return name
}

func splitParts(name string) []string {
	c := CleanPath(name)
	if c == "/" {
		return nil
	}
	return strings.Split(strings.TrimPrefix(c, "/"), "/")
}

func unescapeSegment(s string) string {
	if u, err := url.PathUnescape(s); err == nil {
		return u
	}
	return s
}

func (f *FS) LogicalName(it gpmc.MediaItem) string {
	if ov, ok := f.meta.NameOverride(it.MediaKey); ok {
		return ov
	}
	if d, ok := f.meta.DisguiseFor(it.MediaKey, it.SHA1); ok && d.Version > 0 && d.OriginalName != "" {
		return d.OriginalName
	}
	if orig, ok := disguise.LooksLikeDisguisedFilename(it.Filename); ok {
		return orig
	}
	if it.Filename != "" {
		return it.Filename
	}
	return it.MediaKey
}

func (f *FS) LogicalSize(it gpmc.MediaItem) int64 {
	if d, ok := f.meta.DisguiseFor(it.MediaKey, it.SHA1); ok && d.Version > 0 && d.PayloadSize >= 0 {
		return d.PayloadSize
	}
	return it.SizeBytes
}

func (f *FS) LogicalModTime(it gpmc.MediaItem) time.Time {
	if a, ok := f.meta.GetAttr("key:" + it.MediaKey); ok && a.MtimeNs > 0 {
		return time.Unix(0, a.MtimeNs)
	}
	if !it.Mtime.IsZero() {
		return it.Mtime
	}
	return time.Now()
}

// virtualCapacity is the size reported for the mount: the spoofed Pixel plan has unlimited
// storage, so the real 15 GiB quota is replaced by a large fixed pool.
const virtualCapacity = uint64(40) << 40

func (f *FS) Statfs(ctx context.Context) (QuotaInfo, error) {
	q, err := f.statfsRaw(ctx)
	if err != nil {
		return q, err
	}
	bs := uint64(q.BlockSize)
	if bs == 0 {
		bs = 4096
		q.BlockSize = 4096
	}
	used := min(q.UsedBytes, virtualCapacity)
	q.TotalBytes = virtualCapacity
	q.AvailBytes = virtualCapacity - used
	q.Unlimited = true
	q.TotalBlocks = virtualCapacity / bs
	q.FreeBlocks = q.AvailBytes / bs
	q.AvailBlocks = q.FreeBlocks
	return q, nil
}

func (f *FS) statfsRaw(ctx context.Context) (QuotaInfo, error) {
	f.quotaMu.Lock()
	have := !f.quotaAt.IsZero()
	fresh := have && time.Since(f.quotaAt) < 45*time.Second
	cur := f.quota
	needBg := have && !fresh && !f.quotaRefreshing
	if needBg {
		f.quotaRefreshing = true
	}
	f.quotaMu.Unlock()

	if fresh {
		return cur, nil
	}
	if have {
		if needBg {
			go func() {
				bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, _ = f.refreshQuota(bgCtx)
				f.quotaMu.Lock()
				f.quotaRefreshing = false
				f.quotaMu.Unlock()
			}()
		}
		return cur, nil
	}

	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	q, err := f.refreshQuota(qCtx)
	if err != nil {
		// Fallback virtual 1 TiB pool so statfs never fails mount checks if temporarily offline.
		const fallbackFree = uint64(1) << 40
		const bsize = 4096
		fb := QuotaInfo{
			TotalBytes:  fallbackFree,
			UsedBytes:   0,
			AvailBytes:  fallbackFree,
			Unlimited:   true,
			BlockSize:   bsize,
			TotalBlocks: fallbackFree / bsize,
			FreeBlocks:  fallbackFree / bsize,
			AvailBlocks: fallbackFree / bsize,
			UpdatedAt:   time.Now().UnixMilli(),
		}
		return fb, nil
	}
	return q, nil
}

func (f *FS) refreshQuota(ctx context.Context) (QuotaInfo, error) {
	sq, err := f.gp.GetStorageQuota(ctx)
	if err != nil {
		return QuotaInfo{}, err
	}
	const bsize = 4096
	used := uint64(max(int64(0), sq.UsageBytes))
	var total, avail uint64
	if sq.Unlimited || sq.LimitBytes <= 0 {
		const virtualPool = uint64(15) << 40 // 15 TiB virtual headroom for unlimited accounts
		avail = virtualPool
		total = used + virtualPool
	} else {
		total = uint64(sq.LimitBytes)
		if total > used {
			avail = total - used
		}
	}
	qi := QuotaInfo{
		TotalBytes:      total,
		UsedBytes:       used,
		AvailBytes:      avail,
		PhotosUsedBytes: uint64(max(int64(0), sq.PhotosUsageBytes)),
		Unlimited:       sq.Unlimited,
		BlockSize:       bsize,
		TotalBlocks:     total / bsize,
		FreeBlocks:      avail / bsize,
		AvailBlocks:     avail / bsize,
		UpdatedAt:       time.Now().UnixMilli(),
	}
	f.quotaMu.Lock()
	f.quota = qi
	f.quotaAt = time.Now()
	f.quotaMu.Unlock()
	return qi, nil
}

type resolvedTarget struct {
	clean       string
	isDir       bool
	isSymlink   bool
	symlink     SymlinkEntry
	isFile      bool
	item        gpmc.MediaItem
	ino         uint64
	albumKey    string
	virtualOnly bool
	dirMtime    time.Time
}

func (f *FS) resolve(ctx context.Context, name string, followFinalSymlink bool) (resolvedTarget, error) {
	return f.resolveDepth(ctx, name, followFinalSymlink, 0)
}

func (f *FS) resolveDepth(ctx context.Context, name string, followFinal bool, depth int) (resolvedTarget, error) {
	if depth > 8 {
		return resolvedTarget{}, errors.New("vfs: too many symlink levels")
	}
	c := CleanPath(name)
	if c == "/" {
		return resolvedTarget{clean: "/", isDir: true, ino: 1, dirMtime: time.Now()}, nil
	}

	// Check symlinks first.
	if sym, ok := f.meta.GetSymlink(c); ok {
		if !followFinal {
			return resolvedTarget{clean: c, isSymlink: true, symlink: sym, ino: sym.Ino}, nil
		}
		target := sym.Target
		if !strings.HasPrefix(target, "/") {
			target = path.Join(path.Dir(c), target)
		}
		return f.resolveDepth(ctx, target, true, depth+1)
	}

	// Check virtual directory.
	if mt, ok := f.meta.HasDir(c); ok {
		return resolvedTarget{clean: c, isDir: true, ino: StableIno("dir:" + c), virtualOnly: true, dirMtime: mt}, nil
	}

	// Check virtual / staged / linked file entry.
	if ve, ok := f.meta.GetEntry(c); ok && !f.meta.IsHidden(ve.MediaKey) {
		it, found, _ := f.gp.GetItem(ctx, ve.MediaKey)
		if !found {
			it = gpmc.MediaItem{
				MediaKey:  ve.MediaKey,
				Filename:  path.Base(c),
				SizeBytes: ve.SizeBytes,
				Mtime:     time.Unix(0, ve.CreatedNs),
			}
		}
		parts := splitParts(c)
		albKey := ""
		virt := true
		if len(parts) == 3 && parts[0] == "albums" {
			virt = false
			if alb, aok, _ := f.gp.FindAlbumByTitle(ctx, unescapeSegment(parts[1])); aok {
				albKey = alb.MediaKey
			}
		} else if len(parts) >= 1 && (parts[0] == "all" || parts[0] == "library") {
			virt = false
		}
		return resolvedTarget{
			clean:       c,
			isFile:      true,
			item:        it,
			ino:         ve.Ino,
			albumKey:    albKey,
			virtualOnly: virt,
		}, nil
	}

	parts := splitParts(c)
	switch len(parts) {
	case 1:
		switch parts[0] {
		case "library":
			return resolvedTarget{clean: c, isDir: true, ino: StableIno("dir:/library"), dirMtime: time.Now()}, nil
		case "albums":
			return resolvedTarget{clean: c, isDir: true, ino: StableIno("dir:/albums"), dirMtime: time.Now()}, nil
		case "all":
			if f.cfg.StrictWebDAVRoot {
				return resolvedTarget{}, os.ErrNotExist
			}
			return resolvedTarget{clean: c, isDir: true, ino: StableIno("dir:/all"), dirMtime: time.Now()}, nil
		}
		return resolvedTarget{}, os.ErrNotExist

	case 2:
		switch parts[0] {
		case "all":
			if f.cfg.StrictWebDAVRoot {
				return resolvedTarget{}, os.ErrNotExist
			}
			items, err := f.gp.ListAllItems(ctx)
			if err != nil {
				return resolvedTarget{}, err
			}
			mapped := f.uniqueDisplayItems(items)
			if it, ok := mapped[parts[1]]; ok {
				return resolvedTarget{clean: c, isFile: true, item: it, ino: StableIno("key:" + it.MediaKey)}, nil
			}
			return resolvedTarget{}, os.ErrNotExist

		case "library":
			if _, ok := f.cursorForLabel(ctx, parts[1]); !ok {
				return resolvedTarget{}, os.ErrNotExist
			}
			return resolvedTarget{clean: c, isDir: true, ino: StableIno("dir:" + c), dirMtime: time.Now()}, nil

		case "albums":
			title := unescapeSegment(parts[1])
			album, ok, err := f.gp.FindAlbumByTitle(ctx, title)
			if err != nil {
				return resolvedTarget{}, err
			}
			if !ok {
				return resolvedTarget{}, os.ErrNotExist
			}
			return resolvedTarget{clean: c, isDir: true, ino: StableIno("album:" + album.MediaKey), albumKey: album.MediaKey, dirMtime: album.ModTime()}, nil
		}
		return resolvedTarget{}, os.ErrNotExist

	case 3:
		switch parts[0] {
		case "library":
			cursor, ok := f.cursorForLabel(ctx, parts[1])
			if !ok {
				return resolvedTarget{}, os.ErrNotExist
			}
			page, err := f.gp.ListPage(ctx, cursor)
			if err != nil {
				return resolvedTarget{}, err
			}
			mapped := f.uniqueDisplayItems(page.Items)
			if it, ok := mapped[parts[2]]; ok {
				return resolvedTarget{clean: c, isFile: true, item: it, ino: StableIno("key:" + it.MediaKey)}, nil
			}
			return resolvedTarget{}, os.ErrNotExist

		case "albums":
			title := unescapeSegment(parts[1])
			album, ok, err := f.gp.FindAlbumByTitle(ctx, title)
			if err != nil {
				return resolvedTarget{}, err
			}
			if !ok {
				return resolvedTarget{}, os.ErrNotExist
			}
			items, err := f.albumItems(ctx, album.MediaKey)
			if err != nil {
				return resolvedTarget{}, err
			}
			mapped := f.uniqueDisplayItems(items)
			if it, ok := mapped[parts[2]]; ok {
				return resolvedTarget{clean: c, isFile: true, item: it, ino: StableIno("key:" + it.MediaKey), albumKey: album.MediaKey}, nil
			}
			return resolvedTarget{}, os.ErrNotExist
		}
	}

	return resolvedTarget{}, os.ErrNotExist
}

func albumHideKey(albumKey, mediaKey string) string { return "alb:" + albumKey + "|" + mediaKey }

// albumItems lists an album, minus items unlinked locally: the library snapshot can lag behind
// the album store, so unlinks are remembered here.
func (f *FS) albumItems(ctx context.Context, albumKey string) ([]gpmc.MediaItem, error) {
	items, err := f.gp.ListAlbumItems(ctx, albumKey)
	if err != nil {
		return nil, err
	}
	out := items[:0:0]
	for _, it := range items {
		if !f.meta.IsHidden(albumHideKey(albumKey, it.MediaKey)) {
			out = append(out, it)
		}
	}
	return out, nil
}

func (f *FS) uniqueDisplayItems(items []gpmc.MediaItem) map[string]gpmc.MediaItem {
	out := make(map[string]gpmc.MediaItem, len(items))
	for _, it := range items {
		if f.meta.IsHidden(it.MediaKey) {
			continue
		}
		name := f.LogicalName(it)
		if _, exists := out[name]; exists {
			ext := path.Ext(name)
			stem := strings.TrimSuffix(name, ext)
			short := it.MediaKey
			if len(short) > 8 {
				short = short[len(short)-8:]
			}
			name = fmt.Sprintf("%s (%s)%s", stem, short, ext)
		}
		out[name] = it
	}
	return out
}

func (f *FS) cursorForLabel(ctx context.Context, label string) (string, bool) {
	if label == "recent" || label == "page-1" {
		return "", true
	}
	if !strings.HasPrefix(label, "page-") {
		return "", false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(label, "page-"))
	if err != nil || n < 1 {
		return "", false
	}
	cursor := ""
	for i := 1; i < n; i++ {
		page, err := f.gp.ListPage(ctx, cursor)
		if err != nil || page.NextToken == "" {
			return "", false
		}
		cursor = page.NextToken
	}
	return cursor, true
}

func (f *FS) pageLabels(ctx context.Context) ([]string, error) {
	labels := []string{"recent"}
	cursor := ""
	for i := 2; i <= maxPages; i++ {
		page, err := f.gp.ListPage(ctx, cursor)
		if err != nil {
			return nil, err
		}
		if page.NextToken == "" || len(page.Items) == 0 {
			break
		}
		labels = append(labels, "page-"+strconv.Itoa(i))
		cursor = page.NextToken
	}
	return labels, nil
}

func (f *FS) Stat(ctx context.Context, name string) (*NodeInfo, error) {
	return f.statInternal(ctx, name, true, true)
}

func (f *FS) Lstat(ctx context.Context, name string) (*NodeInfo, error) {
	return f.statInternal(ctx, name, false, true)
}

func (f *FS) statInternal(ctx context.Context, name string, followSymlink bool, probeDisguise bool) (*NodeInfo, error) {
	r, err := f.resolve(ctx, name, followSymlink)
	if err != nil {
		return nil, err
	}
	base := path.Base(r.clean)
	if r.clean == "/" {
		base = "/"
	}

	if r.isSymlink {
		mt := time.Unix(0, r.symlink.MtimeNs)
		ni := &NodeInfo{
			FileName:      base,
			FileSize:      int64(len(r.symlink.Target)),
			FileMode:      fs.ModeSymlink | 0o777,
			Mtime:         mt,
			Atime:         mt,
			Ino:           r.symlink.Ino,
			Nlink:         1,
			UID:           r.symlink.UID,
			GID:           r.symlink.GID,
			SymlinkTarget: r.symlink.Target,
		}
		f.applyPosixAttr("path:"+r.clean, ni)
		return ni, nil
	}

	if r.isDir {
		mt := r.dirMtime
		if mt.IsZero() {
			mt = time.Now()
		}
		ni := &NodeInfo{
			FileName:  base,
			FileMode:  fs.ModeDir | 0o755,
			Mtime:     mt,
			Atime:     mt,
			IsDirFlag: true,
			Ino:       r.ino,
			Nlink:     2,
		}
		f.applyPosixAttr("path:"+r.clean, ni)
		return ni, nil
	}

	// For files whose exact disguise payload size hasn't been cached yet, probe on direct Stat.
	if probeDisguise && r.item.MediaKey != "" {
		if _, cached := f.meta.DisguiseFor(r.item.MediaKey, r.item.SHA1); !cached {
			if _, ok := disguise.LooksLikeDisguisedFilename(r.item.Filename); ok || strings.HasSuffix(strings.ToLower(r.item.Filename), ".mp4") {
				pCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
				_, _ = f.InspectMedia(pCtx, r.item)
				cancel()
			}
		}
	}

	ni := f.buildFileNodeInfo(base, r.clean, r.item, r.ino)
	return ni, nil
}

func (f *FS) buildFileNodeInfo(displayBase, cleanPath string, it gpmc.MediaItem, ino uint64) *NodeInfo {
	if ino == 0 {
		ino = StableIno("key:" + it.MediaKey)
	}
	mt := f.LogicalModTime(it)
	ni := &NodeInfo{
		FileName: displayBase,
		FileSize: f.LogicalSize(it),
		FileMode: 0o644,
		Mtime:    mt,
		Atime:    mt,
		Ino:      ino,
		Nlink:    f.meta.RefCountIno(ino, it.MediaKey),
		MediaKey: it.MediaKey,
	}
	f.applyPosixAttr("key:"+it.MediaKey, ni)
	f.applyPosixAttr("path:"+cleanPath, ni)
	return ni
}

func (f *FS) applyPosixAttr(id string, ni *NodeInfo) {
	a, ok := f.meta.GetAttr(id)
	if !ok {
		return
	}
	if a.HasMode {
		typeBits := ni.FileMode & fs.ModeType
		ni.FileMode = typeBits | fs.FileMode(a.Mode&0o7777)
	}
	if a.HasUID {
		ni.UID = a.UID
	}
	if a.HasGID {
		ni.GID = a.GID
	}
	if a.MtimeNs > 0 {
		ni.Mtime = time.Unix(0, a.MtimeNs)
	}
	if a.AtimeNs > 0 {
		ni.Atime = time.Unix(0, a.AtimeNs)
	}
}

func (f *FS) ReadDir(ctx context.Context, name string) ([]*NodeInfo, error) {
	c := CleanPath(name)
	r, err := f.resolve(ctx, c, true)
	if err != nil {
		return nil, err
	}
	if !r.isDir {
		return nil, errors.New("vfs: not a directory")
	}

	byName := map[string]*NodeInfo{}
	add := func(ni *NodeInfo) {
		if ni != nil && ni.FileName != "" {
			byName[ni.FileName] = ni
		}
	}

	parts := splitParts(c)
	switch {
	case len(parts) == 0:
		add(&NodeInfo{FileName: "library", FileMode: fs.ModeDir | 0o755, Mtime: time.Now(), IsDirFlag: true, Ino: StableIno("dir:/library"), Nlink: 2})
		add(&NodeInfo{FileName: "albums", FileMode: fs.ModeDir | 0o755, Mtime: time.Now(), IsDirFlag: true, Ino: StableIno("dir:/albums"), Nlink: 2})
		if !f.cfg.StrictWebDAVRoot {
			add(&NodeInfo{FileName: "all", FileMode: fs.ModeDir | 0o755, Mtime: time.Now(), IsDirFlag: true, Ino: StableIno("dir:/all"), Nlink: 2})
		}

	case len(parts) == 1 && parts[0] == "all":
		items, err := f.gp.ListAllItems(ctx)
		if err != nil {
			return nil, err
		}
		for dispName, it := range f.uniqueDisplayItems(items) {
			add(f.buildFileNodeInfo(dispName, "/all/"+dispName, it, 0))
		}

	case len(parts) == 1 && parts[0] == "library":
		labels, err := f.pageLabels(ctx)
		if err != nil {
			return nil, err
		}
		for _, l := range labels {
			p := "/library/" + l
			add(&NodeInfo{FileName: l, FileMode: fs.ModeDir | 0o755, Mtime: time.Now(), IsDirFlag: true, Ino: StableIno("dir:" + p), Nlink: 2})
		}

	case len(parts) == 1 && parts[0] == "albums":
		albums, err := f.gp.ListAlbums(ctx)
		if err != nil {
			return nil, err
		}
		for _, a := range albums {
			dname := albumDirName(a)
			if dname == "" {
				continue
			}
			mt := a.ModTime()
			if mt.IsZero() {
				mt = time.Now()
			}
			ni := &NodeInfo{FileName: dname, FileMode: fs.ModeDir | 0o755, Mtime: mt, IsDirFlag: true, Ino: StableIno("album:" + a.MediaKey), Nlink: 2}
			f.applyPosixAttr("path:/albums/"+dname, ni)
			add(ni)
		}

	case len(parts) == 2 && parts[0] == "library":
		cursor, ok := f.cursorForLabel(ctx, parts[1])
		if !ok {
			return nil, os.ErrNotExist
		}
		page, err := f.gp.ListPage(ctx, cursor)
		if err != nil {
			return nil, err
		}
		for dispName, it := range f.uniqueDisplayItems(page.Items) {
			add(f.buildFileNodeInfo(dispName, c+"/"+dispName, it, 0))
		}

	case len(parts) == 2 && parts[0] == "albums":
		title := unescapeSegment(parts[1])
		album, ok, err := f.gp.FindAlbumByTitle(ctx, title)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, os.ErrNotExist
		}
		items, err := f.albumItems(ctx, album.MediaKey)
		if err != nil {
			return nil, err
		}
		for dispName, it := range f.uniqueDisplayItems(items) {
			add(f.buildFileNodeInfo(dispName, c+"/"+dispName, it, 0))
		}
	}

	// Merge custom virtual child directories, entries, and symlinks (except when StrictWebDAVRoot is set on /).
	if !(f.cfg.StrictWebDAVRoot && c == "/") {
		for _, dname := range f.meta.ChildDirs(c) {
			sub := path.Join(c, dname)
			mt, _ := f.meta.HasDir(sub)
			ni := &NodeInfo{FileName: dname, FileMode: fs.ModeDir | 0o755, Mtime: mt, IsDirFlag: true, Ino: StableIno("dir:" + sub), Nlink: 2}
			f.applyPosixAttr("path:"+sub, ni)
			add(ni)
		}
		for ename, ve := range f.meta.ChildEntries(c) {
			if f.meta.IsHidden(ve.MediaKey) {
				continue
			}
			if _, already := byName[ename]; already {
				continue
			}
			sub := path.Join(c, ename)
			it, found, _ := f.gp.GetItem(ctx, ve.MediaKey)
			if !found {
				it = gpmc.MediaItem{
					MediaKey:  ve.MediaKey,
					Filename:  ename,
					SizeBytes: ve.SizeBytes,
					Mtime:     time.Unix(0, ve.CreatedNs),
				}
			}
			add(f.buildFileNodeInfo(ename, sub, it, ve.Ino))
		}
		for sname, sym := range f.meta.ChildSymlinks(c) {
			sub := path.Join(c, sname)
			mt := time.Unix(0, sym.MtimeNs)
			ni := &NodeInfo{
				FileName:      sname,
				FileSize:      int64(len(sym.Target)),
				FileMode:      fs.ModeSymlink | 0o777,
				Mtime:         mt,
				Atime:         mt,
				Ino:           sym.Ino,
				Nlink:         1,
				UID:           sym.UID,
				GID:           sym.GID,
				SymlinkTarget: sym.Target,
			}
			f.applyPosixAttr("path:"+sub, ni)
			add(ni)
		}
	}

	out := make([]*NodeInfo, 0, len(byName))
	for _, ni := range byName {
		out = append(out, ni)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsDirFlag != out[j].IsDirFlag {
			return out[i].IsDirFlag
		}
		return strings.ToLower(out[i].FileName) < strings.ToLower(out[j].FileName)
	})
	return out, nil
}

func (f *FS) Mkdir(ctx context.Context, name string, perm fs.FileMode) error {
	c := CleanPath(name)
	if c == "/" {
		return os.ErrExist
	}
	if _, err := f.resolve(ctx, c, false); err == nil {
		return os.ErrExist
	}
	parts := splitParts(c)
	if len(parts) == 2 && parts[0] == "albums" {
		title := unescapeSegment(parts[1])
		if _, ok, _ := f.gp.FindAlbumByTitle(ctx, title); ok {
			return os.ErrExist
		}
		key, err := f.gp.CreateAlbum(ctx, title, nil)
		if err != nil {
			return err
		}
		if store := f.gp.AlbumStore(); store != nil {
			_ = store.PutAlbum(ctx, albumstore.Album{
				MediaKey:  key,
				Title:     title,
				CreatedAt: time.Now(),
			})
		}
		return nil
	}

	if f.cfg.StrictWebDAVRoot {
		return os.ErrPermission
	}
	if len(parts) >= 1 && (parts[0] == "library" || parts[0] == "albums" || parts[0] == "all") {
		return os.ErrPermission
	}

	parent := path.Dir(c)
	pr, err := f.resolve(ctx, parent, true)
	if err != nil {
		return err
	}
	if !pr.isDir {
		return errors.New("vfs: parent is not a directory")
	}
	if perm == 0 {
		perm = 0o755
	}
	f.meta.PutDir(c, perm)
	return nil
}

func (f *FS) OpenFile(ctx context.Context, name string, flag int, perm fs.FileMode) (*FileHandle, error) {
	c := CleanPath(name)
	parts := splitParts(c)

	r, rErr := f.resolve(ctx, c, true)
	if rErr == nil && r.isDir {
		return nil, errors.New("vfs: is a directory")
	}
	if rErr == nil && flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
		return nil, os.ErrExist
	}
	if rErr != nil && flag&os.O_CREATE == 0 && flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return nil, rErr
	}

	var existing *gpmc.MediaItem
	var existingIno uint64
	albumKey := ""
	virtualOnly := true

	if rErr == nil && r.isFile {
		itCopy := r.item
		existing = &itCopy
		existingIno = r.ino
		albumKey = r.albumKey
		virtualOnly = r.virtualOnly
	} else {
		// Validate parent directory for creation.
		switch {
		case len(parts) == 3 && parts[0] == "albums":
			title := unescapeSegment(parts[1])
			alb, ok, err := f.gp.FindAlbumByTitle(ctx, title)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, os.ErrNotExist
			}
			albumKey = alb.MediaKey
			virtualOnly = false

		case len(parts) == 2 && parts[0] == "all":
			virtualOnly = false

		case len(parts) == 3 && parts[0] == "library":
			if _, ok := f.cursorForLabel(ctx, parts[1]); !ok {
				return nil, os.ErrNotExist
			}
			virtualOnly = false

		default:
			parent := path.Dir(c)
			pr, perr := f.resolve(ctx, parent, true)
			if perr != nil {
				return nil, perr
			}
			if !pr.isDir {
				return nil, errors.New("vfs: parent not a directory")
			}
			virtualOnly = true
		}
	}

	return f.newFileHandle(ctx, c, albumKey, virtualOnly, existing, existingIno, flag, perm)
}

func (f *FS) Remove(ctx context.Context, name string) error {
	c := CleanPath(name)
	if c == "/" || c == "/library" || c == "/albums" || c == "/all" {
		return os.ErrPermission
	}
	if f.meta.DeleteSymlink(c) {
		return nil
	}

	parts := splitParts(c)
	if len(parts) == 2 && parts[0] == "albums" {
		title := unescapeSegment(parts[1])
		album, ok, err := f.gp.FindAlbumByTitle(ctx, title)
		if err != nil {
			return err
		}
		if !ok {
			return os.ErrNotExist
		}
		if store := f.gp.AlbumStore(); store != nil {
			err := store.DeleteAlbum(ctx, album.MediaKey)
			f.gp.InvalidateSyncCache()
			return err
		}
		return os.ErrPermission
	}

	if _, isVirtDir := f.meta.HasDir(c); isVirtDir {
		if f.meta.HasChildren(c) {
			return errors.New("vfs: directory not empty")
		}
		f.meta.DeleteDir(c)
		return nil
	}

	r, err := f.resolve(ctx, c, false)
	if err != nil {
		return err
	}
	if r.isDir {
		return os.ErrPermission
	}

	// If in an album, unlink from album membership.
	if len(parts) == 3 && parts[0] == "albums" && r.albumKey != "" {
		_, _ = f.meta.DeleteEntry(c)
		if store := f.gp.AlbumStore(); store != nil {
			if err := store.RemoveMembers(ctx, r.albumKey, []string{r.item.MediaKey}); err != nil {
				return err
			}
			f.meta.HideKey(albumHideKey(r.albumKey, r.item.MediaKey))
			// The path may resolve through a locally recorded key while the album lists the same
			// file under another key, so hide every listed item carrying this name.
			if listed, lerr := f.gp.ListAlbumItems(ctx, r.albumKey); lerr == nil {
				base := path.Base(c)
				for _, it := range listed {
					if f.LogicalName(it) == base {
						f.meta.HideKey(albumHideKey(r.albumKey, it.MediaKey))
					}
				}
			}
			f.gp.InvalidateSyncCache()
			return nil
		}
		return os.ErrPermission
	}

	// If virtual entry (including hardlinks), decrement link count first.
	if ve, hasVE := f.meta.GetEntry(c); hasVE {
		_, rem := f.meta.DeleteEntry(c)
		if rem > 0 {
			// Another hardlink still points to this MediaKey; keep cloud object alive.
			return nil
		}
		if ve.MediaKey != "" {
			f.InvalidateMedia(ve.MediaKey)
			f.meta.HideKey(ve.MediaKey)
			res, derr := f.gp.DeleteByMediaKeys(ctx, []string{ve.MediaKey}, false)
			if derr != nil {
				return derr
			}
			if e := res[ve.MediaKey]; e != nil && !strings.Contains(e.Error(), "not found") {
				return e
			}
		}
		return nil
	}

	f.InvalidateMedia(r.item.MediaKey)
	f.meta.HideKey(r.item.MediaKey)
	res, err := f.gp.DeleteByMediaKeys(ctx, []string{r.item.MediaKey}, false)
	if err != nil {
		return err
	}
	if e := res[r.item.MediaKey]; e != nil {
		return e
	}
	return nil
}

func (f *FS) RemoveAll(ctx context.Context, name string) error {
	c := CleanPath(name)
	if _, isVirtDir := f.meta.HasDir(c); isVirtDir {
		children, _ := f.ReadDir(ctx, c)
		for _, ch := range children {
			_ = f.RemoveAll(ctx, path.Join(c, ch.FileName))
		}
		f.meta.DeleteDir(c)
		return nil
	}
	return f.Remove(ctx, c)
}

func (f *FS) Rename(ctx context.Context, oldName, newName string) error {
	oldClean := CleanPath(oldName)
	newClean := CleanPath(newName)
	if oldClean == newClean {
		return nil
	}
	if oldClean == "/" || oldClean == "/library" || oldClean == "/albums" || oldClean == "/all" {
		return os.ErrPermission
	}

	// Symlink rename.
	if sym, ok := f.meta.GetSymlink(oldClean); ok {
		_ = f.Remove(ctx, newClean)
		f.meta.DeleteSymlink(oldClean)
		f.meta.PutSymlink(newClean, sym.Target, sym.UID, sym.GID)
		return nil
	}

	// Virtual directory rename.
	if _, ok := f.meta.HasDir(oldClean); ok {
		if strings.HasPrefix(newClean, oldClean+"/") {
			return errors.New("vfs: cannot move directory into itself")
		}
		f.meta.RenameSubtree(oldClean, newClean)
		return nil
	}

	r, err := f.resolve(ctx, oldClean, false)
	if err != nil {
		return err
	}

	// Album directory rename (for local albums).
	oldParts := splitParts(oldClean)
	newParts := splitParts(newClean)
	if r.isDir && len(oldParts) == 2 && oldParts[0] == "albums" && len(newParts) == 2 && newParts[0] == "albums" {
		if store := f.gp.AlbumStore(); store != nil && r.albumKey != "" {
			if alb, err := store.GetAlbum(ctx, r.albumKey); err == nil && alb.MediaKey != "" {
				alb.Title = unescapeSegment(newParts[1])
				_ = store.PutAlbum(ctx, alb)
				f.gp.InvalidateSyncCache()
				return nil
			}
		}
		return os.ErrPermission
	}
	if !r.isFile {
		return os.ErrPermission
	}

	// Overwrite target if it already exists.
	if _, err := f.resolve(ctx, newClean, false); err == nil {
		_ = f.Remove(ctx, newClean)
	}

	newBase := path.Base(newClean)

	// Moving into an album.
	if len(newParts) == 3 && newParts[0] == "albums" {
		alb, ok, err := f.gp.FindAlbumByTitle(ctx, unescapeSegment(newParts[1]))
		if err != nil {
			return err
		}
		if !ok {
			return os.ErrNotExist
		}
		if alb.MediaKey != r.albumKey {
			if err := f.gp.AddMediaToAlbum(ctx, alb.MediaKey, []string{r.item.MediaKey}); err != nil {
				return err
			}
			if store := f.gp.AlbumStore(); store != nil {
				_ = store.AddMembers(ctx, alb.MediaKey, []string{r.item.MediaKey})
			}
			if r.albumKey != "" {
				if store := f.gp.AlbumStore(); store != nil {
					_ = store.RemoveMembers(ctx, r.albumKey, []string{r.item.MediaKey})
				}
			}
		}
		if newBase != path.Base(oldClean) {
			f.meta.SetNameOverride(r.item.MediaKey, newBase)
		}
		_, _ = f.meta.DeleteEntry(oldClean)
		f.meta.PutEntry(newClean, VirtualEntry{MediaKey: r.item.MediaKey, SizeBytes: f.LogicalSize(r.item), Ino: r.ino})
		return nil
	}

	// Moving/renaming within /all or /library.
	if len(newParts) >= 2 && (newParts[0] == "all" || newParts[0] == "library") {
		f.meta.SetNameOverride(r.item.MediaKey, newBase)
		if r.virtualOnly {
			_, _ = f.meta.DeleteEntry(oldClean)
		}
		return nil
	}

	// Moving into a virtual POSIX path (/foo.txt or /mydir/foo.txt).
	parent := path.Dir(newClean)
	pr, err := f.resolve(ctx, parent, true)
	if err != nil {
		return err
	}
	if !pr.isDir {
		return errors.New("vfs: destination parent is not a directory")
	}
	if _, hasVE := f.meta.GetEntry(oldClean); hasVE {
		f.meta.RenameSubtree(oldClean, newClean)
	} else {
		f.meta.PutEntry(newClean, VirtualEntry{
			MediaKey:  r.item.MediaKey,
			SizeBytes: f.LogicalSize(r.item),
			Ino:       r.ino,
		})
	}
	return nil
}

func (f *FS) Chmod(ctx context.Context, name string, mode fs.FileMode) error {
	c := CleanPath(name)
	r, err := f.resolve(ctx, c, true)
	if err != nil {
		return err
	}
	id := "path:" + c
	if r.isFile && r.item.MediaKey != "" {
		id = "key:" + r.item.MediaKey
	}
	f.meta.UpdateAttr(id, func(a *PosixAttr) {
		a.HasMode = true
		a.Mode = uint32(mode & 0o7777)
	})
	return nil
}

func (f *FS) Chown(ctx context.Context, name string, uid, gid int) error {
	c := CleanPath(name)
	r, err := f.resolve(ctx, c, false)
	if err != nil {
		return err
	}
	id := "path:" + c
	if r.isFile && r.item.MediaKey != "" {
		id = "key:" + r.item.MediaKey
	}
	f.meta.UpdateAttr(id, func(a *PosixAttr) {
		if uid >= 0 {
			a.HasUID = true
			a.UID = uint32(uid)
		}
		if gid >= 0 {
			a.HasGID = true
			a.GID = uint32(gid)
		}
	})
	return nil
}

func (f *FS) Utimens(ctx context.Context, name string, atime, mtime *time.Time) error {
	c := CleanPath(name)
	r, err := f.resolve(ctx, c, false)
	if err != nil {
		return err
	}
	id := "path:" + c
	if r.isFile && r.item.MediaKey != "" {
		id = "key:" + r.item.MediaKey
	}
	f.meta.UpdateAttr(id, func(a *PosixAttr) {
		if atime != nil {
			a.AtimeNs = atime.UnixNano()
		}
		if mtime != nil {
			a.MtimeNs = mtime.UnixNano()
		}
	})
	return nil
}

func (f *FS) Truncate(ctx context.Context, name string, size int64) error {
	fh, err := f.OpenFile(ctx, name, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := fh.Truncate(size); err != nil {
		_ = fh.Close()
		return err
	}
	return fh.Close()
}

func (f *FS) Symlink(ctx context.Context, target, linkPath string, uid, gid uint32) (*NodeInfo, error) {
	c := CleanPath(linkPath)
	if c == "/" {
		return nil, os.ErrExist
	}
	if _, err := f.resolve(ctx, c, false); err == nil {
		return nil, os.ErrExist
	}
	parent := path.Dir(c)
	pr, err := f.resolve(ctx, parent, true)
	if err != nil {
		return nil, err
	}
	if !pr.isDir {
		return nil, errors.New("vfs: parent not a directory")
	}
	sym := f.meta.PutSymlink(c, target, uid, gid)
	mt := time.Unix(0, sym.MtimeNs)
	return &NodeInfo{
		FileName:      path.Base(c),
		FileSize:      int64(len(target)),
		FileMode:      fs.ModeSymlink | 0o777,
		Mtime:         mt,
		Atime:         mt,
		Ino:           sym.Ino,
		Nlink:         1,
		UID:           uid,
		GID:           gid,
		SymlinkTarget: target,
	}, nil
}

func (f *FS) Readlink(ctx context.Context, linkPath string) (string, error) {
	c := CleanPath(linkPath)
	sym, ok := f.meta.GetSymlink(c)
	if !ok {
		return "", os.ErrInvalid
	}
	return sym.Target, nil
}

func (f *FS) Link(ctx context.Context, oldName, newName string) (*NodeInfo, error) {
	oldClean := CleanPath(oldName)
	newClean := CleanPath(newName)
	r, err := f.resolve(ctx, oldClean, false)
	if err != nil {
		return nil, err
	}
	if !r.isFile {
		return nil, os.ErrPermission
	}
	if _, err := f.resolve(ctx, newClean, false); err == nil {
		return nil, os.ErrExist
	}
	parent := path.Dir(newClean)
	pr, err := f.resolve(ctx, parent, true)
	if err != nil {
		return nil, err
	}
	if !pr.isDir {
		return nil, errors.New("vfs: parent not a directory")
	}

	ino := r.ino
	if ino == 0 {
		ino = StableIno("key:" + r.item.MediaKey)
	}
	// Ensure source also has a tracked entry so RefCountIno >= 2.
	if _, hasOld := f.meta.GetEntry(oldClean); !hasOld {
		f.meta.PutEntry(oldClean, VirtualEntry{
			MediaKey:  r.item.MediaKey,
			SizeBytes: f.LogicalSize(r.item),
			Ino:       ino,
		})
	}
	f.meta.PutEntry(newClean, VirtualEntry{
		MediaKey:  r.item.MediaKey,
		SizeBytes: f.LogicalSize(r.item),
		Ino:       ino,
	})
	return f.Stat(ctx, newClean)
}

func albumDirName(a gpmc.Album) string {
	t := strings.TrimSpace(a.Title)
	if t == "" {
		return ""
	}
	return strings.ReplaceAll(t, "/", "-")
}
