package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"gpix/pkg/disguise"
	"gpix/pkg/gpmc"
)

type SeekableReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
	Size() int64
	Name() string
	ModTime() time.Time
	IsDisguised() bool
	IsEncrypted() bool
}

func (f *FS) InspectMedia(ctx context.Context, item gpmc.MediaItem) (DisguiseMeta, error) {
	if item.MediaKey == "" {
		return DisguiseMeta{}, errors.New("vfs: empty media key")
	}
	if d, ok := f.meta.GetDisguise(item.MediaKey); ok {
		return d, nil
	}

	head := make([]byte, 64*1024)
	n, err := f.chunks.ReadPhysicalAt(ctx, f.gp.HTTPClient(), item.MediaKey, head, 0, item.SizeBytes)
	if err != nil && err != io.EOF {
		return DisguiseMeta{}, err
	}
	head = head[:n]

	ver, hdr, payloadOff, perr := disguise.PeekMeta(head)
	var dm DisguiseMeta
	if perr == nil && ver > 0 {
		dm = DisguiseMeta{
			Checked:       true,
			Version:       ver,
			OriginalName:  hdr.Filename,
			PayloadSize:   hdr.PayloadSize,
			PayloadOffset: int64(payloadOff),
		}
	} else {
		dm = DisguiseMeta{
			Checked:     true,
			Version:     0,
			PayloadSize: item.SizeBytes,
		}
	}
	f.meta.PutDisguise(item.MediaKey, dm)
	return dm, nil
}

func (f *FS) OpenReader(ctx context.Context, item gpmc.MediaItem, passphraseOverride string) (SeekableReader, error) {
	dm, err := f.InspectMedia(ctx, item)
	if err != nil {
		return nil, err
	}

	name := f.LogicalName(item)
	mtime := f.LogicalModTime(item)

	switch dm.Version {
	case 0:
		size := item.SizeBytes
		return &chunkBackedReader{
			fs:          f,
			mediaKey:    item.MediaKey,
			name:        name,
			mtime:       mtime,
			physOffset:  0,
			logicalSize: size,
			physLimit:   size,
		}, nil

	case 1:
		if dm.OriginalName != "" {
			if _, hasOverride := f.meta.NameOverride(item.MediaKey); !hasOverride {
				name = dm.OriginalName
			}
		}
		physLimit := dm.PayloadOffset + dm.PayloadSize
		return &chunkBackedReader{
			fs:          f,
			mediaKey:    item.MediaKey,
			name:        name,
			mtime:       mtime,
			physOffset:  dm.PayloadOffset,
			logicalSize: dm.PayloadSize,
			physLimit:   physLimit,
			disguised:   true,
		}, nil

	case 2:
		if dm.OriginalName != "" {
			if _, hasOverride := f.meta.NameOverride(item.MediaKey); !hasOverride {
				name = dm.OriginalName
			}
		}
		pw := passphraseOverride
		if pw == "" {
			pw = f.cfg.EncPassphrase
		}
		if pw == "" {
			return nil, disguise.ErrEncrypted
		}
		filePath, ok := f.decCache.Get(item.MediaKey)
		if !ok {
			var derr error
			filePath, derr = f.decryptToCache(ctx, item.MediaKey, pw)
			if derr != nil {
				return nil, derr
			}
		}
		osFile, err := os.Open(filePath)
		if err != nil {
			return nil, err
		}
		st, err := osFile.Stat()
		if err != nil {
			osFile.Close()
			return nil, err
		}
		return &osFileReader{
			f:     osFile,
			name:  name,
			size:  st.Size(),
			mtime: mtime,
		}, nil

	default:
		return nil, fmt.Errorf("vfs: unsupported disguise version %d", dm.Version)
	}
}

func (f *FS) decryptToCache(ctx context.Context, mediaKey, passphrase string) (string, error) {
	for attempt := 0; attempt < 2; attempt++ {
		dlURL, err := f.urls.Get(ctx, mediaKey)
		if err != nil {
			return "", err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, dlURL, nil)
		if err != nil {
			return "", err
		}
		resp, err := f.gp.HTTPClient().Do(req)
		if err != nil {
			return "", err
		}
		if (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusGone) && attempt == 0 {
			resp.Body.Close()
			f.urls.Invalidate(mediaKey)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return "", fmt.Errorf("vfs: decrypt download status %d", resp.StatusCode)
		}
		_, payload, err := disguise.ExtractWithPassphrase(resp.Body, passphrase)
		resp.Body.Close()
		if err != nil {
			return "", err
		}
		return f.decCache.Put(mediaKey, payload)
	}
	return "", fmt.Errorf("vfs: decrypt download failed for %s", mediaKey)
}

type chunkBackedReader struct {
	fs          *FS
	mediaKey    string
	name        string
	mtime       time.Time
	physOffset  int64
	logicalSize int64
	physLimit   int64
	disguised   bool

	mu  sync.Mutex
	pos int64
}

func (r *chunkBackedReader) Size() int64        { return r.logicalSize }
func (r *chunkBackedReader) Name() string       { return r.name }
func (r *chunkBackedReader) ModTime() time.Time { return r.mtime }
func (r *chunkBackedReader) IsDisguised() bool  { return r.disguised }
func (r *chunkBackedReader) IsEncrypted() bool  { return false }
func (r *chunkBackedReader) Close() error       { return nil }

func (r *chunkBackedReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("vfs: negative offset")
	}
	if r.logicalSize >= 0 && off >= r.logicalSize {
		return 0, io.EOF
	}
	want := len(p)
	if r.logicalSize > 0 && off+int64(want) > r.logicalSize {
		want = int(r.logicalSize - off)
	}
	n, err := r.fs.chunks.ReadPhysicalAt(context.Background(), r.fs.gp.HTTPClient(), r.mediaKey, p[:want], r.physOffset+off, r.physLimit)
	if err == nil && n < len(p) && r.logicalSize > 0 && off+int64(n) >= r.logicalSize {
		return n, io.EOF
	}
	return n, err
}

func (r *chunkBackedReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, err := r.ReadAt(p, r.pos)
	r.pos += int64(n)
	return n, err
}

func (r *chunkBackedReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		abs = r.logicalSize + offset
	default:
		return 0, errors.New("vfs: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("vfs: negative seek position")
	}
	r.pos = abs
	return abs, nil
}

type osFileReader struct {
	f     *os.File
	name  string
	size  int64
	mtime time.Time
}

func (o *osFileReader) Read(p []byte) (int, error)              { return o.f.Read(p) }
func (o *osFileReader) ReadAt(p []byte, off int64) (int, error) { return o.f.ReadAt(p, off) }
func (o *osFileReader) Seek(off int64, w int) (int64, error)    { return o.f.Seek(off, w) }
func (o *osFileReader) Close() error                            { return o.f.Close() }
func (o *osFileReader) Size() int64                             { return o.size }
func (o *osFileReader) Name() string                            { return o.name }
func (o *osFileReader) ModTime() time.Time                      { return o.mtime }
func (o *osFileReader) IsDisguised() bool                       { return true }
func (o *osFileReader) IsEncrypted() bool                       { return true }
