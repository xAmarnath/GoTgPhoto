package webdav

import (
	"context"
	"errors"
	"io/fs"
	"os"

	"golang.org/x/net/webdav"

	"gpix/pkg/vfs"
)

type gpixFS struct {
	v   *vfs.FS
	cfg Config
}

func newFS(v *vfs.FS, cfg Config) *gpixFS {
	return &gpixFS{v: v, cfg: cfg}
}

func (f *gpixFS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	if err := f.v.Mkdir(ctx, name, perm); err != nil {
		if errors.Is(err, os.ErrPermission) {
			return webdav.ErrForbidden
		}
		return err
	}
	return nil
}

func (f *gpixFS) Rename(ctx context.Context, oldName, newName string) error {
	if err := f.v.Rename(ctx, oldName, newName); err != nil {
		if errors.Is(err, os.ErrPermission) {
			return webdav.ErrForbidden
		}
		return err
	}
	return nil
}

func (f *gpixFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	return f.v.Stat(ctx, name)
}

func (f *gpixFS) RemoveAll(ctx context.Context, name string) error {
	if err := f.v.RemoveAll(ctx, name); err != nil {
		if errors.Is(err, os.ErrPermission) {
			return webdav.ErrForbidden
		}
		return err
	}
	return nil
}

func (f *gpixFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	clean := vfs.CleanPath(name)
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) == 0 {
		if st, err := f.v.Stat(ctx, clean); err == nil && st.IsDir() {
			return &davDir{v: f.v, path: clean, info: st}, nil
		}
	}
	return f.v.OpenFile(ctx, clean, flag, perm)
}

var errIsDir = errors.New("webdav: is a directory")

type davDir struct {
	v    *vfs.FS
	path string
	info fs.FileInfo
	done bool
}

func (d *davDir) Close() error                   { return nil }
func (d *davDir) Read([]byte) (int, error)       { return 0, errIsDir }
func (d *davDir) Write([]byte) (int, error)      { return 0, errIsDir }
func (d *davDir) Seek(int64, int) (int64, error) { return 0, errIsDir }
func (d *davDir) Stat() (fs.FileInfo, error)     { return d.info, nil }

func (d *davDir) Readdir(count int) ([]fs.FileInfo, error) {
	if d.done && count > 0 {
		return nil, nil
	}
	nodes, err := d.v.ReadDir(context.Background(), d.path)
	if err != nil {
		return nil, err
	}
	d.done = true
	infos := make([]fs.FileInfo, len(nodes))
	for i, n := range nodes {
		infos[i] = n
	}
	if count > 0 && count < len(infos) {
		infos = infos[:count]
	}
	return infos, nil
}
