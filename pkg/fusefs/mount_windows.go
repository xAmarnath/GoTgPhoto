//go:build windows

package fusefs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/winfsp/cgofuse/fuse"

	"gpix/pkg/vfs"
)

const Supported = true

// winHandle runs a WinFsp host; the mount point is a drive letter ("X:") or an
// empty, non-existent directory path.
type winHandle struct {
	host       *fuse.FileSystemHost
	mountPoint string
	done       chan struct{}
	once       sync.Once
}

func (h *winHandle) Wait()              { <-h.done }
func (h *winHandle) MountPoint() string { return h.mountPoint }
func (h *winHandle) Unmount() error {
	var err error
	h.once.Do(func() {
		if !h.host.Unmount() {
			err = errors.New("fusefs: winfsp unmount failed")
		}
	})
	return err
}

func mountOS(ctx context.Context, v *vfs.FS, opts Options) (Handle, error) {
	if opts.MountPoint == "" {
		return nil, errors.New("fusefs: mount point is required (e.g. X:)")
	}
	g := &winFS{v: v, handles: map[uint64]*lazyFile{}, ready: make(chan struct{}), dirs: map[string]*dirEntry{}, fresh: map[string]time.Time{}}
	host := fuse.NewFileSystemHost(g)
	host.SetCapReaddirPlus(true)
	host.SetCapDeleteAccess(true)

	args := []string{
		"-o", "volname=New GDisk", "-o", "FileSystemName=gpix", "-o", "uid=-1,gid=-1",
		"-o", "FileInfoTimeout=3000", "-o", "DirInfoTimeout=3000", "-o", "VolumeInfoTimeout=30000",
	}
	if opts.ReadOnly {
		args = append(args, "-o", "ro")
	}
	if opts.AllowOther {
		args = append(args, "-o", "allow_other")
	}
	if opts.Debug {
		args = append(args, "-d")
	}

	h := &winHandle{host: host, mountPoint: opts.MountPoint, done: make(chan struct{})}
	failed := make(chan struct{})
	go func() {
		if !host.Mount(opts.MountPoint, args) {
			close(failed)
		}
		close(h.done)
	}()

	select {
	case <-g.ready:
	case <-failed:
		return nil, fmt.Errorf("fusefs: WinFsp mount of %q failed (is WinFsp installed and the drive letter free?)", opts.MountPoint)
	case <-time.After(15 * time.Second):
		_ = h.Unmount()
		return nil, errors.New("fusefs: WinFsp mount timed out")
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = h.Unmount()
		case <-h.done:
		}
	}()
	return h, nil
}

type winFS struct {
	fuse.FileSystemBase
	v     *vfs.FS
	ready chan struct{}
	once  sync.Once

	mu      sync.Mutex
	handles map[uint64]*lazyFile
	nextFH  atomic.Uint64

	dmu    sync.Mutex
	saving bool
	dirs   map[string]*dirEntry
	fresh  map[string]time.Time
}

// dirEntry is a cached directory listing. Per-file Lstat on the "all" and "library" views is
// very slow (seconds), so Getattr is answered from the parent's listing instead.
type dirEntry struct {
	done       chan struct{}
	items      map[string]*vfs.NodeInfo
	err        error
	at         time.Time
	refreshing bool
}

const (
	dirTTL   = 10 * time.Second
	staleMax = 24 * time.Hour
	freshTTL = 60 * time.Second
)

// listDir serves from cache stale-while-revalidate: a listing younger than dirTTL is returned
// as-is, an older one (up to staleMax, including ones loaded from disk) is returned immediately
// while a background refresh runs, and only a never-seen directory blocks on the backend.
func (g *winFS) listDir(dir string) (map[string]*vfs.NodeInfo, error) {
	g.dmu.Lock()
	e, ok := g.dirs[dir]
	if ok {
		select {
		case <-e.done:
			age := time.Since(e.at)
			if e.err == nil && age < staleMax {
				if age >= dirTTL && !e.refreshing {
					e.refreshing = true
					go g.refreshDir(dir)
				}
				g.dmu.Unlock()
				return e.items, nil
			}
		default:
			g.dmu.Unlock()
			<-e.done
			return e.items, e.err
		}
	}
	g.dmu.Unlock()
	return g.loadDir(dir)
}

func (g *winFS) loadDir(dir string) (map[string]*vfs.NodeInfo, error) {
	e := &dirEntry{done: make(chan struct{})}
	g.dmu.Lock()
	g.dirs[dir] = e
	g.dmu.Unlock()

	list, err := g.v.ReadDir(g.ctx(), dir)
	e.err = err
	if err == nil {
		e.items = make(map[string]*vfs.NodeInfo, len(list))
		for _, it := range list {
			e.items[it.FileName] = it
		}
	}
	e.at = time.Now()
	close(e.done)
	if err == nil {
		g.markDirty()
	}
	return e.items, e.err
}

func (g *winFS) refreshDir(dir string) {
	list, err := g.v.ReadDir(g.ctx(), dir)
	g.dmu.Lock()
	defer g.dmu.Unlock()
	e, ok := g.dirs[dir]
	if !ok {
		return
	}
	e.refreshing = false
	if err != nil {
		return
	}
	m := make(map[string]*vfs.NodeInfo, len(list))
	for _, it := range list {
		m[it.FileName] = it
	}
	e.items, e.at = m, time.Now()
	g.markDirtyLocked()
}

type diskDir struct {
	At    time.Time
	Items []*vfs.NodeInfo
}

func cacheFile() string {
	d, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return filepath.Join(d, "gpix", "dircache.json")
}

func (g *winFS) loadDiskCache() {
	f := cacheFile()
	if f == "" {
		return
	}
	b, err := os.ReadFile(f)
	if err != nil {
		return
	}
	var m map[string]diskDir
	if json.Unmarshal(b, &m) != nil {
		return
	}
	g.dmu.Lock()
	defer g.dmu.Unlock()
	for dir, d := range m {
		e := &dirEntry{done: make(chan struct{}), at: d.At, items: make(map[string]*vfs.NodeInfo, len(d.Items))}
		close(e.done)
		for _, it := range d.Items {
			e.items[it.FileName] = it
		}
		g.dirs[dir] = e
	}
}

func (g *winFS) markDirty() {
	g.dmu.Lock()
	defer g.dmu.Unlock()
	g.markDirtyLocked()
}

// markDirtyLocked schedules a debounced write of the listings to disk.
func (g *winFS) markDirtyLocked() {
	if g.saving {
		return
	}
	g.saving = true
	time.AfterFunc(5*time.Second, g.saveDiskCache)
}

func (g *winFS) saveDiskCache() {
	g.dmu.Lock()
	g.saving = false
	m := make(map[string]diskDir, len(g.dirs))
	for dir, e := range g.dirs {
		select {
		case <-e.done:
		default:
			continue
		}
		if e.err != nil || e.items == nil {
			continue
		}
		d := diskDir{At: e.at, Items: make([]*vfs.NodeInfo, 0, len(e.items))}
		for _, it := range e.items {
			d.Items = append(d.Items, it)
		}
		m[dir] = d
	}
	g.dmu.Unlock()
	f := cacheFile()
	if f == "" {
		return
	}
	b, err := json.Marshal(m)
	if err != nil || os.MkdirAll(filepath.Dir(f), 0o755) != nil {
		return
	}
	tmp := f + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		_ = os.Rename(tmp, f)
	}
}

// prewarm refreshes the top of the tree so the first Explorer open is already cached.
func (g *winFS) prewarm() {
	for _, d := range []string{"/", "/albums", "/all", "/library"} {
		if items, err := g.listDir(d); err == nil && d == "/library" {
			for name, it := range items {
				if it.IsDirFlag {
					_, _ = g.listDir(path.Join(d, name))
				}
			}
		}
	}
}

// invalidate drops cached listings touched by a mutation and remembers the path so Getattr
// falls back to a direct Lstat while the backend catches up.
func (g *winFS) invalidate(paths ...string) {
	g.dmu.Lock()
	defer g.dmu.Unlock()
	now := time.Now()
	for _, p := range paths {
		delete(g.dirs, p)
		delete(g.dirs, path.Dir(p))
		g.fresh[p] = now
	}
	for k, t := range g.fresh {
		if now.Sub(t) > freshTTL {
			delete(g.fresh, k)
		}
	}
}

func (g *winFS) isFresh(p string) bool {
	g.dmu.Lock()
	defer g.dmu.Unlock()
	t, ok := g.fresh[p]
	return ok && time.Since(t) < freshTTL
}

func (g *winFS) lstat(p string) (*vfs.NodeInfo, error) {
	if p == "/" || g.isFresh(p) {
		return g.v.Lstat(g.ctx(), p)
	}
	items, err := g.listDir(path.Dir(p))
	if err != nil {
		return g.v.Lstat(g.ctx(), p)
	}
	if it, ok := items[path.Base(p)]; ok {
		cp := *it
		return &cp, nil
	}
	return nil, os.ErrNotExist
}

func (g *winFS) Init() {
	g.once.Do(func() {
		g.loadDiskCache()
		close(g.ready)
		go g.prewarm()
	})
}

func (g *winFS) ctx() context.Context { return context.Background() }

func cp(name string) string { return vfs.CleanPath(strings.ReplaceAll(name, "\\", "/")) }

func errc(err error) int {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, os.ErrNotExist), errors.Is(err, fs.ErrNotExist):
		return -fuse.ENOENT
	case errors.Is(err, os.ErrExist), errors.Is(err, fs.ErrExist):
		return -fuse.EEXIST
	case errors.Is(err, os.ErrPermission), errors.Is(err, fs.ErrPermission):
		return -fuse.EACCES
	case errors.Is(err, os.ErrInvalid), errors.Is(err, fs.ErrInvalid):
		return -fuse.EINVAL
	case err.Error() == "vfs: directory not empty":
		return -fuse.ENOTEMPTY
	}
	return -fuse.EIO
}

func fillStat(ni *vfs.NodeInfo, st *fuse.Stat_t) {
	size := max(int64(0), ni.FileSize)
	st.Ino = ni.Ino
	st.Size = size
	st.Blksize = 4096
	st.Blocks = (size + 511) / 512
	st.Nlink = ni.Nlink
	if st.Nlink == 0 {
		st.Nlink = 1
	}
	perm := uint32(ni.FileMode.Perm())
	switch {
	case ni.FileMode&fs.ModeSymlink != 0:
		st.Mode = fuse.S_IFLNK | 0o777
	case ni.IsDirFlag:
		st.Mode = fuse.S_IFDIR | perm
	default:
		st.Mode = fuse.S_IFREG | perm
	}
	mt := ni.Mtime
	if mt.IsZero() {
		mt = time.Now()
	}
	at := ni.Atime
	if at.IsZero() {
		at = mt
	}
	st.Atim = fuse.NewTimespec(at)
	st.Mtim = fuse.NewTimespec(mt)
	st.Ctim = st.Mtim
	st.Birthtim = st.Mtim
}

// lazyFile defers vfs.OpenFile until the first data access. WinFsp opens a handle for every
// CreateFile, including the metadata-only opens Explorer does per item, and a real open costs
// a network round trip per file.
type lazyFile struct {
	g     *winFS
	path  string
	flags int
	size  int64

	mu sync.Mutex
	fh *vfs.FileHandle
}

func (l *lazyFile) get() (*vfs.FileHandle, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fh == nil {
		fh, err := l.g.v.OpenFile(l.g.ctx(), l.path, l.flags, 0o644)
		if err != nil {
			return nil, err
		}
		l.fh = fh
	}
	return l.fh, nil
}

func (l *lazyFile) opened() *vfs.FileHandle {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fh
}

func (l *lazyFile) Size() int64 {
	if fh := l.opened(); fh != nil {
		return fh.Size()
	}
	return l.size
}

func (l *lazyFile) Truncate(n int64) error {
	fh, err := l.get()
	if err != nil {
		return err
	}
	return fh.Truncate(n)
}

func (l *lazyFile) ReadAt(p []byte, off int64) (int, error) {
	fh, err := l.get()
	if err != nil {
		return 0, err
	}
	return fh.ReadAt(p, off)
}

func (l *lazyFile) WriteAt(p []byte, off int64) (int, error) {
	fh, err := l.get()
	if err != nil {
		return 0, err
	}
	return fh.WriteAt(p, off)
}

func (l *lazyFile) Flush() error {
	if fh := l.opened(); fh != nil {
		return fh.Flush()
	}
	return nil
}

func (l *lazyFile) Sync() error {
	if fh := l.opened(); fh != nil {
		return fh.Sync()
	}
	return nil
}

func (l *lazyFile) Close() error {
	if fh := l.opened(); fh != nil {
		return fh.Close()
	}
	return nil
}

func (g *winFS) handle(fh uint64) *lazyFile {
	if fh == 0 || fh == ^uint64(0) {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.handles[fh]
}

func (g *winFS) addHandle(h *lazyFile) uint64 {
	id := g.nextFH.Add(1)
	g.mu.Lock()
	g.handles[id] = h
	g.mu.Unlock()
	return id
}

func (g *winFS) Statfs(_ string, st *fuse.Statfs_t) int {
	qi, err := g.v.Statfs(g.ctx())
	if err != nil {
		return errc(err)
	}
	st.Bsize = uint64(qi.BlockSize)
	st.Frsize = uint64(qi.BlockSize)
	st.Blocks = qi.TotalBlocks
	st.Bfree = qi.FreeBlocks
	st.Bavail = qi.AvailBlocks
	st.Files = 1_000_000
	st.Ffree = 999_000
	st.Namemax = 255
	return 0
}

func (g *winFS) Getattr(name string, st *fuse.Stat_t, fh uint64) int {
	p := cp(name)
	h := g.handle(fh)
	ni, err := g.lstat(p)
	if err != nil {
		// A freshly created file is not listed until its first commit; the open handle is authoritative.
		if h == nil {
			return errc(err)
		}
		ni = &vfs.NodeInfo{FileName: path.Base(p), FileMode: 0o644, Mtime: time.Now(), Ino: vfs.StableIno(p), Nlink: 1}
	}
	if h != nil && !ni.IsDirFlag {
		ni.FileSize = h.Size()
	}
	fillStat(ni, st)
	return 0
}

func (g *winFS) Access(name string, _ uint32) int {
	_, err := g.lstat(cp(name))
	return errc(err)
}

func (g *winFS) Readdir(name string, fill func(string, *fuse.Stat_t, int64) bool, _ int64, _ uint64) int {
	items, err := g.listDir(cp(name))
	if err != nil {
		return errc(err)
	}
	fill(".", nil, 0)
	fill("..", nil, 0)
	for _, it := range items {
		var st fuse.Stat_t
		fillStat(it, &st)
		if !fill(it.FileName, &st, 0) {
			break
		}
	}
	return 0
}

func (g *winFS) Open(name string, flags int) (int, uint64) {
	p := cp(name)
	ni, err := g.lstat(p)
	if err != nil {
		return errc(err), ^uint64(0)
	}
	if ni.IsDirFlag {
		return 0, ^uint64(0)
	}
	if flags&(fuse.O_WRONLY|fuse.O_RDWR|fuse.O_TRUNC|fuse.O_APPEND) == 0 {
		return 0, g.addHandle(&lazyFile{g: g, path: p, flags: flags, size: ni.FileSize})
	}
	fh, err := g.v.OpenFile(g.ctx(), p, flags, 0o644)
	if err != nil {
		return errc(err), ^uint64(0)
	}
	return 0, g.addHandle(&lazyFile{g: g, path: p, flags: flags, fh: fh})
}

func (g *winFS) Create(name string, flags int, mode uint32) (int, uint64) {
	perm := fs.FileMode(mode & 0o7777)
	if perm == 0 {
		perm = 0o644
	}
	fh, err := g.v.OpenFile(g.ctx(), cp(name), flags|os.O_CREATE, perm)
	if err != nil {
		return errc(err), ^uint64(0)
	}
	g.invalidate(cp(name))
	return 0, g.addHandle(&lazyFile{g: g, path: cp(name), flags: flags, fh: fh})
}

func (g *winFS) Mkdir(name string, mode uint32) int {
	perm := fs.FileMode(mode & 0o7777)
	if perm == 0 {
		perm = 0o755
	}
	defer g.invalidate(cp(name))
	return errc(g.v.Mkdir(g.ctx(), cp(name), perm))
}

func (g *winFS) Unlink(name string) int {
	defer g.invalidate(cp(name))
	return errc(g.v.Remove(g.ctx(), cp(name)))
}

func (g *winFS) Rmdir(name string) int {
	defer g.invalidate(cp(name))
	return errc(g.v.Remove(g.ctx(), cp(name)))
}

func (g *winFS) Rename(oldName, newName string) int {
	defer g.invalidate(cp(oldName), cp(newName))
	return errc(g.v.Rename(g.ctx(), cp(oldName), cp(newName)))
}

func (g *winFS) Symlink(target, newName string) int {
	defer g.invalidate(cp(newName))
	_, err := g.v.Symlink(g.ctx(), target, cp(newName), 0, 0)
	return errc(err)
}

func (g *winFS) Readlink(name string) (int, string) {
	t, err := g.v.Readlink(g.ctx(), cp(name))
	if err != nil {
		return errc(err), ""
	}
	return 0, t
}

func (g *winFS) Link(oldName, newName string) int {
	defer g.invalidate(cp(newName))
	_, err := g.v.Link(g.ctx(), cp(oldName), cp(newName))
	return errc(err)
}

func (g *winFS) Chmod(name string, mode uint32) int {
	defer g.invalidate(cp(name))
	return errc(g.v.Chmod(g.ctx(), cp(name), fs.FileMode(mode&0o7777)))
}

func (g *winFS) Chown(string, uint32, uint32) int    { return 0 }
func (g *winFS) Chflags(string, uint32) int          { return 0 }
func (g *winFS) Setcrtime(string, fuse.Timespec) int { return 0 }

func (g *winFS) Utimens(name string, tmsp []fuse.Timespec) int {
	if len(tmsp) < 2 {
		return 0
	}
	pick := func(t fuse.Timespec) *time.Time {
		switch t.Nsec {
		case fuse.UTIME_OMIT:
			return nil
		case fuse.UTIME_NOW:
			n := time.Now()
			return &n
		}
		x := t.Time()
		return &x
	}
	at, mt := pick(tmsp[0]), pick(tmsp[1])
	if at == nil && mt == nil {
		return 0
	}
	defer g.invalidate(cp(name))
	return errc(g.v.Utimens(g.ctx(), cp(name), at, mt))
}

func (g *winFS) Truncate(name string, size int64, fh uint64) int {
	if h := g.handle(fh); h != nil {
		return errc(h.Truncate(size))
	}
	defer g.invalidate(cp(name))
	return errc(g.v.Truncate(g.ctx(), cp(name), size))
}

func (g *winFS) Read(_ string, buff []byte, ofst int64, fh uint64) int {
	h := g.handle(fh)
	if h == nil {
		return -fuse.EBADF
	}
	n, err := h.ReadAt(buff, ofst)
	if err != nil && !errors.Is(err, io.EOF) {
		return errc(err)
	}
	return n
}

func (g *winFS) Write(_ string, buff []byte, ofst int64, fh uint64) int {
	h := g.handle(fh)
	if h == nil {
		return -fuse.EBADF
	}
	n, err := h.WriteAt(buff, ofst)
	if err != nil {
		return errc(err)
	}
	return n
}

func (g *winFS) Flush(name string, fh uint64) int {
	if h := g.handle(fh); h != nil {
		defer g.invalidate(cp(name))
		return errc(h.Flush())
	}
	return 0
}

func (g *winFS) Fsync(_ string, _ bool, fh uint64) int {
	if h := g.handle(fh); h != nil {
		return errc(h.Sync())
	}
	return 0
}

func (g *winFS) Release(name string, fh uint64) int {
	g.mu.Lock()
	h := g.handles[fh]
	delete(g.handles, fh)
	g.mu.Unlock()
	if h == nil {
		return 0
	}
	defer g.invalidate(cp(name))
	return errc(h.Close())
}
