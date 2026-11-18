package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"gpix/pkg/disguise"
	"gpix/pkg/gpmc"
)

// FileHandle supports full POSIX random-access Read, ReadAt, Write, WriteAt,
// Seek, Truncate, Fsync, Flush, and Close via Copy-on-Write local staging.
type FileHandle struct {
	fs          *FS
	cleanPath   string
	filename    string
	albumKey    string
	virtualOnly bool // true if path is a virtual POSIX path (root or custom dir)
	flags       int
	perm        fs.FileMode

	existing    *gpmc.MediaItem
	existingIno uint64

	mu       sync.Mutex
	reader   SeekableReader
	staged   *os.File
	pos      int64
	dirty    bool
	closed   bool
	uploaded string // latest committed mediaKey
}

func (f *FS) newFileHandle(ctx context.Context, cleanPath string, albumKey string, virtualOnly bool, existing *gpmc.MediaItem, existingIno uint64, flags int, perm fs.FileMode) (*FileHandle, error) {
	fh := &FileHandle{
		fs:          f,
		cleanPath:   cleanPath,
		filename:    path.Base(cleanPath),
		albumKey:    albumKey,
		virtualOnly: virtualOnly,
		flags:       flags,
		perm:        perm,
		existing:    existing,
		existingIno: existingIno,
	}

	writable := flags&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0
	truncateNow := flags&os.O_TRUNC != 0 || existing == nil

	if writable && truncateNow {
		if err := fh.initEmptyStage(); err != nil {
			return nil, err
		}
		fh.dirty = true
		return fh, nil
	}

	if existing != nil && flags&os.O_APPEND != 0 {
		if err := fh.materializeLocked(ctx); err != nil {
			return nil, err
		}
		st, err := fh.staged.Stat()
		if err == nil {
			fh.pos = st.Size()
		}
	}
	return fh, nil
}

func (fh *FileHandle) initEmptyStage() error {
	if err := os.MkdirAll(fh.fs.stagingDir(), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(fh.fs.stagingDir(), "gpix-stage-*")
	if err != nil {
		return err
	}
	atomic.AddInt64(&fh.fs.activeStaged, 1)
	fh.staged = tmp
	return nil
}

func (fh *FileHandle) materializeLocked(ctx context.Context) error {
	if fh.staged != nil {
		return nil
	}
	if err := fh.initEmptyStage(); err != nil {
		return err
	}
	if fh.existing == nil {
		return nil
	}
	sr, err := fh.fs.OpenReader(ctx, *fh.existing, "")
	if err != nil {
		fh.cleanupStageLocked()
		return err
	}
	defer sr.Close()
	if _, err := io.Copy(fh.staged, sr); err != nil {
		fh.cleanupStageLocked()
		return err
	}
	_, _ = fh.staged.Seek(fh.pos, io.SeekStart)
	return nil
}

func (fh *FileHandle) ensureReaderLocked(ctx context.Context) error {
	if fh.reader != nil {
		return nil
	}
	if fh.existing == nil {
		return os.ErrNotExist
	}
	sr, err := fh.fs.OpenReader(ctx, *fh.existing, "")
	if err != nil {
		return err
	}
	fh.reader = sr
	return nil
}

func (fh *FileHandle) ReadAt(p []byte, off int64) (int, error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return 0, os.ErrClosed
	}
	if fh.staged != nil {
		return fh.staged.ReadAt(p, off)
	}
	if err := fh.ensureReaderLocked(context.Background()); err != nil {
		return 0, err
	}
	return fh.reader.ReadAt(p, off)
}

func (fh *FileHandle) Read(p []byte) (int, error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return 0, os.ErrClosed
	}
	var n int
	var err error
	if fh.staged != nil {
		n, err = fh.staged.ReadAt(p, fh.pos)
	} else {
		if e := fh.ensureReaderLocked(context.Background()); e != nil {
			return 0, e
		}
		n, err = fh.reader.ReadAt(p, fh.pos)
	}
	fh.pos += int64(n)
	return n, err
}

func (fh *FileHandle) WriteAt(p []byte, off int64) (int, error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return 0, os.ErrClosed
	}
	if fh.flags&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_APPEND) == 0 {
		return 0, os.ErrPermission
	}
	if err := fh.materializeLocked(context.Background()); err != nil {
		return 0, err
	}
	writeOff := off
	if fh.flags&os.O_APPEND != 0 {
		if st, err := fh.staged.Stat(); err == nil {
			writeOff = st.Size()
		}
	}
	n, err := fh.staged.WriteAt(p, writeOff)
	if n > 0 {
		fh.dirty = true
	}
	return n, err
}

func (fh *FileHandle) Write(p []byte) (int, error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return 0, os.ErrClosed
	}
	if fh.flags&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_APPEND) == 0 {
		return 0, os.ErrPermission
	}
	if err := fh.materializeLocked(context.Background()); err != nil {
		return 0, err
	}
	writeOff := fh.pos
	if fh.flags&os.O_APPEND != 0 {
		if st, err := fh.staged.Stat(); err == nil {
			writeOff = st.Size()
		}
	}
	n, err := fh.staged.WriteAt(p, writeOff)
	if n > 0 {
		fh.pos = writeOff + int64(n)
		fh.dirty = true
	}
	return n, err
}

func (fh *FileHandle) Truncate(size int64) error {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return os.ErrClosed
	}
	if err := fh.materializeLocked(context.Background()); err != nil {
		return err
	}
	if err := fh.staged.Truncate(size); err != nil {
		return err
	}
	if fh.pos > size {
		fh.pos = size
	}
	fh.dirty = true
	return nil
}

func (fh *FileHandle) Seek(offset int64, whence int) (int64, error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return 0, os.ErrClosed
	}
	curSize := fh.sizeLocked()
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = fh.pos + offset
	case io.SeekEnd:
		abs = curSize + offset
	default:
		return 0, errors.New("vfs: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("vfs: negative seek")
	}
	fh.pos = abs
	return abs, nil
}

func (fh *FileHandle) Size() int64 {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	return fh.sizeLocked()
}

func (fh *FileHandle) sizeLocked() int64 {
	if fh.staged != nil {
		if st, err := fh.staged.Stat(); err == nil {
			return st.Size()
		}
	}
	if fh.reader != nil {
		return fh.reader.Size()
	}
	if fh.existing != nil {
		return fh.fs.LogicalSize(*fh.existing)
	}
	return 0
}

func (fh *FileHandle) Stat() (fs.FileInfo, error) {
	fh.mu.Lock()
	sz := fh.sizeLocked()
	fh.mu.Unlock()
	mode := fh.perm
	if mode == 0 {
		mode = 0o644
	}
	return &NodeInfo{
		FileName: fh.filename,
		FileSize: sz,
		FileMode: mode,
		Mtime:    time.Now(),
		Ino:      StableIno(fh.cleanPath),
		Nlink:    1,
	}, nil
}

func (fh *FileHandle) Readdir(int) ([]fs.FileInfo, error) {
	return nil, errors.New("vfs: not a directory")
}

func (fh *FileHandle) Sync() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	return fh.commitLocked(context.Background())
}

func (fh *FileHandle) Flush() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	return fh.commitLocked(context.Background())
}

func (fh *FileHandle) Close() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return nil
	}
	err := fh.commitLocked(context.Background())
	fh.closed = true
	if fh.reader != nil {
		_ = fh.reader.Close()
		fh.reader = nil
	}
	fh.cleanupStageLocked()
	return err
}

func (fh *FileHandle) cleanupStageLocked() {
	if fh.staged != nil {
		name := fh.staged.Name()
		_ = fh.staged.Close()
		_ = os.Remove(name)
		fh.staged = nil
		atomic.AddInt64(&fh.fs.activeStaged, -1)
	}
}

func (fh *FileHandle) commitLocked(ctx context.Context) error {
	if !fh.dirty || fh.staged == nil {
		return nil
	}
	if err := fh.staged.Sync(); err != nil {
		return err
	}
	st, err := fh.staged.Stat()
	if err != nil {
		return err
	}
	logicalSize := st.Size()
	stagePath := fh.staged.Name()

	head := make([]byte, 512)
	nHead, _ := fh.staged.ReadAt(head, 0)
	head = head[:nHead]

	uploadPath := stagePath
	commitName := fh.filename
	disguiseVer := 0
	var wrappedTmp string

	// Google Photos rejects 0-byte media or arbitrary non-media formats unless wrapped in disguise MP4.
	if logicalSize == 0 || disguise.ShouldWrap("", fh.filename, head) {
		var werr error
		wrappedTmp, disguiseVer, werr = wrapStageFile(fh.fs.stagingDir(), stagePath, fh.filename, logicalSize, fh.fs.cfg.EncPassphrase)
		if werr != nil {
			return fmt.Errorf("vfs: wrap %s: %w", fh.filename, werr)
		}
		defer os.Remove(wrappedTmp)
		uploadPath = wrappedTmp
		commitName = fh.filename + ".mp4"
	}

	res, err := fh.fs.gp.UploadFile(ctx, uploadPath, gpmc.UploadOpts{
		Quality:      fh.fs.cfg.Quality,
		Force:        fh.existing != nil,
		OverrideName: commitName,
	})
	if err != nil {
		return fmt.Errorf("vfs: upload %s: %w", fh.filename, err)
	}

	newKey := res.MediaKey
	fh.uploaded = newKey
	fh.fs.meta.UnhideKey(newKey)

	if disguiseVer > 0 {
		// Probe local wrapped file header so exact offset & size are cached immediately.
		var wHead [64 * 1024]byte
		if wf, oerr := os.Open(uploadPath); oerr == nil {
			wn, _ := io.ReadFull(wf, wHead[:])
			_ = wf.Close()
			if ver, hdr, pOff, perr := disguise.PeekMeta(wHead[:wn]); perr == nil && ver > 0 {
				fh.fs.meta.PutDisguise(newKey, DisguiseMeta{
					Checked:       true,
					Version:       ver,
					OriginalName:  hdr.Filename,
					PayloadSize:   hdr.PayloadSize,
					PayloadOffset: int64(pOff),
				})
			}
		}
	} else {
		fh.fs.meta.PutDisguise(newKey, DisguiseMeta{
			Checked:     true,
			Version:     0,
			PayloadSize: logicalSize,
		})
	}

	if fh.perm != 0 {
		fh.fs.meta.UpdateAttr("key:"+newKey, func(a *PosixAttr) {
			if !a.HasMode {
				a.HasMode = true
				a.Mode = uint32(fh.perm.Perm())
			}
			a.MtimeNs = time.Now().UnixNano()
		})
	}

	if fh.albumKey != "" && newKey != "" {
		_ = fh.fs.gp.AddMediaToAlbum(ctx, fh.albumKey, []string{newKey})
		if store := fh.fs.gp.AlbumStore(); store != nil {
			_ = store.AddMembers(ctx, fh.albumKey, []string{newKey})
		}
	}

	if fh.existingIno != 0 && fh.fs.meta.UpdateInodeMediaKey(fh.existingIno, newKey, logicalSize) > 0 {
		// Updated all hardlinks sharing this POSIX inode.
	} else {
		ino := fh.existingIno
		if ino == 0 {
			if ve, ok := fh.fs.meta.GetEntry(fh.cleanPath); ok {
				ino = ve.Ino
			}
		}
		fh.fs.meta.PutEntry(fh.cleanPath, VirtualEntry{
			MediaKey:  newKey,
			SizeBytes: logicalSize,
			Ino:       ino,
		})
	}

	// If this overwrote a previous MediaKey and nothing else references the old key, remove old version.
	if fh.existing != nil && fh.existing.MediaKey != "" && fh.existing.MediaKey != newKey {
		oldKey := fh.existing.MediaKey
		fh.fs.chunks.EvictMedia(oldKey)
		fh.fs.decCache.Delete(oldKey)
		if fh.albumKey != "" {
			if store := fh.fs.gp.AlbumStore(); store != nil {
				_ = store.RemoveMembers(ctx, fh.albumKey, []string{oldKey})
			}
		}
		if fh.fs.meta.CountMediaKeyRefs(oldKey) == 0 {
			fh.fs.meta.HideKey(oldKey)
			_, _ = fh.fs.gp.DeleteByMediaKeys(ctx, []string{oldKey}, false)
		}
	}

	fh.existing = &gpmc.MediaItem{
		MediaKey:  newKey,
		Filename:  commitName,
		SizeBytes: logicalSize,
		Mtime:     time.Now(),
	}
	fh.dirty = false
	fh.fs.gp.InvalidateSyncCache()
	return nil
}

func wrapStageFile(dir, srcPath, origName string, payloadSize int64, passphrase string) (string, int, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return "", 0, err
	}
	defer src.Close()

	out, err := os.CreateTemp(dir, "gpix-wrap-*.mp4")
	if err != nil {
		return "", 0, err
	}
	var wrapped io.Reader
	ver := 1
	if passphrase != "" {
		var werr error
		wrapped, _, werr = disguise.WrapEncrypted(origName, src, passphrase)
		if werr != nil {
			out.Close()
			_ = os.Remove(out.Name())
			return "", 0, werr
		}
		ver = 2
	} else {
		wrapped, _ = disguise.Wrap(origName, src, payloadSize)
	}
	if _, err := io.Copy(out, wrapped); err != nil {
		out.Close()
		_ = os.Remove(out.Name())
		return "", 0, err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(out.Name())
		return "", 0, err
	}
	return out.Name(), ver, nil
}
