package gpmc

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Stage int

const (
	StageHash Stage = iota
	StageDedup
	StageGetToken
	StagePut
	StageCommit
	StageDone
)

func (s Stage) String() string {
	switch s {
	case StageHash:
		return "hash"
	case StageDedup:
		return "dedup"
	case StageGetToken:
		return "get-token"
	case StagePut:
		return "put"
	case StageCommit:
		return "commit"
	case StageDone:
		return "done"
	}
	return "?"
}

type UploadResult struct {
	Path     string
	SHA1     []byte
	MediaKey string
	Skipped  bool
	Err      error
}

type UploadEvent struct {
	Path       string
	Stage      Stage
	BytesDone  int64
	BytesTotal int64
	Err        error
}

func (c *Client) UploadFile(ctx context.Context, path string, opts UploadOpts) (UploadResult, error) {
	return c.uploadOne(ctx, path, opts, nil)
}

func (c *Client) UploadFileWithProgress(ctx context.Context, path string, opts UploadOpts, progress func(UploadEvent)) (UploadResult, error) {
	return c.uploadOne(ctx, path, opts, progress)
}

func (c *Client) uploadOne(ctx context.Context, path string, opts UploadOpts, progress func(UploadEvent)) (UploadResult, error) {
	st, err := os.Stat(path)
	if err != nil {
		if progress != nil {
			progress(UploadEvent{Path: path, Stage: StageHash, Err: err})
		}
		return UploadResult{Path: path, Err: err}, err
	}
	if st.IsDir() {
		err := errors.New("gpmc: uploadOne called on a directory")
		return UploadResult{Path: path, Err: err}, err
	}
	name := filepath.Base(path)
	if opts.OverrideName != "" {
		name = opts.OverrideName
	}
	src := uploadSource{
		label: path,
		name:  name,
		size:  st.Size(),
		mtime: st.ModTime(),
		open:  func() (io.ReadCloser, error) { return os.Open(path) },
	}
	res, err := c.uploadSource(ctx, src, opts, progress)
	if err == nil && opts.DeleteAfter {
		_ = os.Remove(path)
	}
	return res, err
}

// UploadStream uploads content produced by open (called once for hashing and again for each
// PUT attempt) without requiring it to exist as a single file on disk.
func (c *Client) UploadStream(ctx context.Context, name string, size int64, mtime time.Time, open func() (io.ReadCloser, error), opts UploadOpts) (UploadResult, error) {
	if opts.OverrideName != "" {
		name = opts.OverrideName
	}
	return c.uploadSource(ctx, uploadSource{label: name, name: name, size: size, mtime: mtime, open: open}, opts, nil)
}

type uploadSource struct {
	label string
	name  string
	size  int64
	mtime time.Time
	open  func() (io.ReadCloser, error)
}

func hashSource(open func() (io.ReadCloser, error)) ([]byte, string, error) {
	rc, err := open()
	if err != nil {
		return nil, "", err
	}
	defer rc.Close()
	h := sha1.New()
	if _, err := io.CopyBuffer(h, rc, make([]byte, 1<<20)); err != nil {
		return nil, "", err
	}
	sum := h.Sum(nil)
	return sum, base64.StdEncoding.EncodeToString(sum), nil
}

func (c *Client) uploadSource(ctx context.Context, src uploadSource, opts UploadOpts, progress func(UploadEvent)) (UploadResult, error) {
	emit := func(s Stage, done, total int64, err error) {
		if progress != nil {
			progress(UploadEvent{Path: src.label, Stage: s, BytesDone: done, BytesTotal: total, Err: err})
		}
	}
	res := UploadResult{Path: src.label}
	size := src.size
	fail := func(s Stage, done int64, err error) (UploadResult, error) {
		res.Err = err
		emit(s, done, size, err)
		return res, err
	}

	emit(StageHash, 0, size, nil)
	digest, b64, err := hashSource(src.open)
	if err != nil {
		return fail(StageHash, 0, err)
	}

	res.SHA1 = digest

	if !opts.Force {
		emit(StageDedup, 0, size, nil)
		key, found, err := c.findRemoteMediaByHash(ctx, digest)
		if err != nil {
			return fail(StageDedup, 0, err)
		}
		if found {
			res.MediaKey = key
			res.Skipped = true
			emit(StageDone, size, size, nil)
			return res, nil
		}
	}

	emit(StageGetToken, 0, size, nil)
	uploadID, err := c.getUploadToken(ctx, b64, size)
	if err != nil {
		return fail(StageGetToken, 0, err)
	}

	emit(StagePut, 0, size, nil)
	receipt, err := c.putFile(ctx, uploadID, func() (io.ReadCloser, error) {
		rc, err := src.open()
		if err != nil || progress == nil {
			return rc, err
		}
		return &progressReader{rc: rc, total: size, emit: func(done int64) { emit(StagePut, done, size, nil) }}, nil
	}, size)
	if err != nil {
		return fail(StagePut, 0, err)
	}

	emit(StageCommit, size, size, nil)
	key, err := c.commitUpload(ctx, receipt, src.name, digest, opts.Quality, c.profile, src.mtime)
	if err != nil {
		return fail(StageCommit, size, err)
	}

	res.MediaKey = key
	c.InvalidateSyncCache()
	emit(StageDone, size, size, nil)
	return res, nil
}

func (c *Client) UploadFiles(ctx context.Context, paths []string, opts UploadOpts, progress func(UploadEvent)) ([]UploadResult, error) {
	expanded, err := expandPaths(paths, opts.Recursive)
	if err != nil {
		return nil, err
	}
	conc := opts.Concurrency
	if conc < 1 {
		conc = 1
	}

	results := make([]UploadResult, len(expanded))
	sem := make(chan struct{}, conc)

	var evMu sync.Mutex
	emit := func(ev UploadEvent) {
		if progress == nil {
			return
		}
		evMu.Lock()
		defer evMu.Unlock()
		progress(ev)
	}

	var wg sync.WaitGroup
	for i, p := range expanded {
		i, p := i, p
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			r, _ := c.uploadOne(ctx, p, opts, emit)
			results[i] = r
		}()
	}
	wg.Wait()
	return results, nil
}

func expandPaths(paths []string, recursive bool) ([]string, error) {
	var out []string
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if !st.IsDir() {
			out = append(out, p)
			continue
		}
		if !recursive {
			entries, err := os.ReadDir(p)
			if err != nil {
				return nil, err
			}
			for _, e := range entries {
				if !e.IsDir() {
					out = append(out, filepath.Join(p, e.Name()))
				}
			}
			continue
		}
		err = filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				out = append(out, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

var _ = time.Now
