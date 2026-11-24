package fusefs

import (
	"context"
	"errors"
	"log/slog"

	"gpix/pkg/vfs"
)

var ErrUnsupported = errors.New("fusefs: mounting is only supported on Linux (FUSE) and Windows (WinFsp)")

type Options struct {
	MountPoint string
	AllowOther bool
	ReadOnly   bool
	Debug      bool
	Logger     *slog.Logger
}

type Status struct {
	Supported  bool   `json:"supported"`
	Mounted    bool   `json:"mounted"`
	MountPoint string `json:"mountPoint"`
	AllowOther bool   `json:"allowOther"`
	Error      string `json:"error,omitempty"`
}

type Handle interface {
	Wait()
	Unmount() error
	MountPoint() string
}

// Mount mounts the given VFS at opts.MountPoint and automatically unmounts when ctx is canceled.
func Mount(ctx context.Context, v *vfs.FS, opts Options) (Handle, error) {
	return mountOS(ctx, v, opts)
}
