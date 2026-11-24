//go:build !linux && !windows

package fusefs

import (
	"context"

	"gpix/pkg/vfs"
)

const Supported = false

func mountOS(_ context.Context, _ *vfs.FS, _ Options) (Handle, error) {
	return nil, ErrUnsupported
}
