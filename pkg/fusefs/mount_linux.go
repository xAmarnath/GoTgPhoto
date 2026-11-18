//go:build linux

package fusefs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"gpix/pkg/vfs"
)

const (
	Supported               = true
	fuseOpcodeCanonicalPath = 2016
)

type linuxHandle struct {
	srv        *fuse.Server
	mountPoint string
	devFuseFD  int
	bridgeFD   int
	once       sync.Once
}

func (h *linuxHandle) Wait()              { h.srv.Wait() }
func (h *linuxHandle) MountPoint() string { return h.mountPoint }
func (h *linuxHandle) Unmount() error {
	var err error
	h.once.Do(func() {
		err = syscall.Unmount(h.mountPoint, 0)
		if err != nil {
			err = syscall.Unmount(h.mountPoint, syscall.MNT_DETACH)
		}
		_ = syscall.Close(h.devFuseFD)
		_ = syscall.Close(h.bridgeFD)
	})
	return err
}

// openBridgedDevFuse opens /dev/fuse, mounts fuse.gpix at mountPoint, and returns a
// SOCK_SEQPACKET fd for go-fuse while safely intercepting Android/MTK kernel 4.4's
// custom FUSE_CANONICAL_PATH (opcode 2016), whose kernel handler underflows path[size-1]
// if standard ENOSYS (size=0) is returned.
func openBridgedDevFuse(mountPoint string, allowOther, readOnly bool, maxWrite int) (devFD int, bridgeFD int, goFuseFD int, err error) {
	devFD, err = syscall.Open("/dev/fuse", os.O_RDWR|syscall.O_CLOEXEC, 0)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("open /dev/fuse: %w", err)
	}

	var st syscall.Stat_t
	if err = syscall.Stat(mountPoint, &st); err != nil {
		_ = syscall.Close(devFD)
		return -1, -1, -1, err
	}

	flags := uintptr(syscall.MS_NOSUID | syscall.MS_NODEV)
	if readOnly {
		flags |= syscall.MS_RDONLY
	}

	mountOpts := []string{
		fmt.Sprintf("fd=%d", devFD),
		fmt.Sprintf("rootmode=%o", st.Mode&syscall.S_IFMT),
		fmt.Sprintf("user_id=%d", os.Geteuid()),
		fmt.Sprintf("group_id=%d", os.Getegid()),
		fmt.Sprintf("max_read=%d", maxWrite),
		"default_permissions",
	}
	if allowOther {
		mountOpts = append(mountOpts, "allow_other")
	}

	err = syscall.Mount("gpix", mountPoint, "fuse.gpix", flags, strings.Join(mountOpts, ","))
	if err != nil && allowOther {
		mountOpts = mountOpts[:len(mountOpts)-1]
		err = syscall.Mount("gpix", mountPoint, "fuse.gpix", flags, strings.Join(mountOpts, ","))
	}
	if err != nil {
		_ = syscall.Close(devFD)
		return -1, -1, -1, fmt.Errorf("mount fuse.gpix on %s: %w", mountPoint, err)
	}

	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		_ = syscall.Unmount(mountPoint, syscall.MNT_DETACH)
		_ = syscall.Close(devFD)
		return -1, -1, -1, fmt.Errorf("socketpair: %w", err)
	}
	const sockBuf = 2 * 1024 * 1024
	for _, sfd := range pair {
		_ = syscall.SetsockoptInt(sfd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, sockBuf)
		_ = syscall.SetsockoptInt(sfd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, sockBuf)
	}

	bridgeFD, goFuseFD = pair[0], pair[1]

	// Pump requests: /dev/fuse -> go-fuse (intercepting opcode 2016 FUSE_CANONICAL_PATH)
	go func() {
		buf := make([]byte, maxWrite+8192)
		// "/dev/null/gpix\x00" resolves on devtmpfs with -ENOTDIR so MTK kernel 4.4's
		// kern_path() safely falls through to default_path without out-of-bounds write or recursion.
		canonicalFallback := []byte("/dev/null/gpix\x00")
		for {
			n, rerr := syscall.Read(devFD, buf)
			if rerr != nil {
				if errors.Is(rerr, syscall.EINTR) {
					continue
				}
				_ = syscall.Close(bridgeFD)
				return
			}
			if n >= 16 {
				opcode := binary.LittleEndian.Uint32(buf[4:8])
				if opcode == fuseOpcodeCanonicalPath {
					unique := binary.LittleEndian.Uint64(buf[8:16])
					reply := make([]byte, 16+len(canonicalFallback))
					binary.LittleEndian.PutUint32(reply[0:4], uint32(len(reply)))
					binary.LittleEndian.PutUint32(reply[4:8], 0) // oh.error = 0
					binary.LittleEndian.PutUint64(reply[8:16], unique)
					copy(reply[16:], canonicalFallback)
					_, _ = syscall.Write(devFD, reply)
					continue
				}
			}
			if _, werr := syscall.Write(bridgeFD, buf[:n]); werr != nil {
				return
			}
		}
	}()

	// Pump replies: go-fuse -> /dev/fuse
	go func() {
		outBuf := make([]byte, maxWrite+8192)
		for {
			n, rerr := syscall.Read(bridgeFD, outBuf)
			if rerr != nil {
				if errors.Is(rerr, syscall.EINTR) {
					continue
				}
				return
			}
			if n > 0 {
				_, _ = syscall.Write(devFD, outBuf[:n])
			}
		}
	}()

	return devFD, bridgeFD, goFuseFD, nil
}

func mountOS(ctx context.Context, v *vfs.FS, opts Options) (Handle, error) {
	if opts.MountPoint == "" {
		return nil, errors.New("fusefs: mount point is required")
	}
	if err := os.MkdirAll(opts.MountPoint, 0o755); err != nil {
		return nil, err
	}

	const maxWrite = 128 * 1024
	devFD, bridgeFD, goFuseFD, err := openBridgedDevFuse(opts.MountPoint, opts.AllowOther, opts.ReadOnly, maxWrite)
	if err != nil {
		return nil, err
	}

	root := &gpixNode{v: v, cleanPath: "/"}
	attrTimeout := 5 * time.Second
	entryTimeout := 5 * time.Second

	fsOpts := &gofusefs.Options{
		AttrTimeout:  &attrTimeout,
		EntryTimeout: &entryTimeout,
		MountOptions: fuse.MountOptions{
			FsName:        "gpix",
			Name:          "gpix",
			MaxBackground: 32,
			MaxWrite:      maxWrite,
			DisableSplice: true,
			Debug:         opts.Debug,
		},
	}

	magicMount := fmt.Sprintf("/dev/fd/%d", goFuseFD)
	srv, err := gofusefs.Mount(magicMount, root, fsOpts)
	if err != nil {
		_ = syscall.Close(goFuseFD)
		_ = syscall.Close(bridgeFD)
		_ = syscall.Unmount(opts.MountPoint, syscall.MNT_DETACH)
		_ = syscall.Close(devFD)
		return nil, err
	}

	h := &linuxHandle{
		srv:        srv,
		mountPoint: opts.MountPoint,
		devFuseFD:  devFD,
		bridgeFD:   bridgeFD,
	}
	go func() {
		<-ctx.Done()
		_ = h.Unmount()
	}()
	return h, nil
}

type gpixNode struct {
	gofusefs.Inode
	v         *vfs.FS
	cleanPath string
}

var (
	_ = (gofusefs.NodeGetattrer)((*gpixNode)(nil))
	_ = (gofusefs.NodeSetattrer)((*gpixNode)(nil))
	_ = (gofusefs.NodeLookuper)((*gpixNode)(nil))
	_ = (gofusefs.NodeReaddirer)((*gpixNode)(nil))
	_ = (gofusefs.NodeOpener)((*gpixNode)(nil))
	_ = (gofusefs.NodeCreater)((*gpixNode)(nil))
	_ = (gofusefs.NodeMkdirer)((*gpixNode)(nil))
	_ = (gofusefs.NodeRmdirer)((*gpixNode)(nil))
	_ = (gofusefs.NodeUnlinker)((*gpixNode)(nil))
	_ = (gofusefs.NodeRenamer)((*gpixNode)(nil))
	_ = (gofusefs.NodeSymlinker)((*gpixNode)(nil))
	_ = (gofusefs.NodeReadlinker)((*gpixNode)(nil))
	_ = (gofusefs.NodeLinker)((*gpixNode)(nil))
	_ = (gofusefs.NodeStatfser)((*gpixNode)(nil))
)

func (n *gpixNode) currentPath() string {
	p := "/" + n.Path(n.Root())
	return vfs.CleanPath(p)
}

func fillAttr(ni *vfs.NodeInfo, out *fuse.Attr) {
	out.Ino = ni.Ino
	out.Size = uint64(max(int64(0), ni.FileSize))
	out.Blocks = (out.Size + 511) / 512
	out.Blksize = 4096
	out.Nlink = ni.Nlink
	if out.Nlink == 0 {
		out.Nlink = 1
	}
	out.Uid = ni.UID
	out.Gid = ni.GID

	perm := uint32(ni.FileMode.Perm())
	switch {
	case ni.FileMode&fs.ModeSymlink != 0:
		out.Mode = syscall.S_IFLNK | 0o777
	case ni.IsDirFlag:
		out.Mode = syscall.S_IFDIR | perm
	default:
		out.Mode = syscall.S_IFREG | perm
	}

	mt := ni.Mtime
	if mt.IsZero() {
		mt = time.Now()
	}
	at := ni.Atime
	if at.IsZero() {
		at = mt
	}
	out.SetTimes(&at, &mt, &mt)
}

func nodeStableAttr(ni *vfs.NodeInfo) gofusefs.StableAttr {
	mode := uint32(syscall.S_IFREG)
	if ni.FileMode&fs.ModeSymlink != 0 {
		mode = syscall.S_IFLNK
	} else if ni.IsDirFlag {
		mode = syscall.S_IFDIR
	}
	return gofusefs.StableAttr{
		Mode: mode,
		Ino:  ni.Ino,
	}
}

func toErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	switch {
	case errors.Is(err, os.ErrNotExist), errors.Is(err, fs.ErrNotExist):
		return syscall.ENOENT
	case errors.Is(err, os.ErrExist), errors.Is(err, fs.ErrExist):
		return syscall.EEXIST
	case errors.Is(err, os.ErrPermission), errors.Is(err, fs.ErrPermission):
		return syscall.EACCES
	case errors.Is(err, os.ErrInvalid), errors.Is(err, fs.ErrInvalid):
		return syscall.EINVAL
	default:
		return syscall.EIO
	}
}

func (n *gpixNode) Getattr(ctx context.Context, fh gofusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if h, ok := fh.(*fuseHandle); ok {
		return h.Getattr(ctx, out)
	}
	ni, err := n.v.Lstat(ctx, n.currentPath())
	if err != nil {
		return toErrno(err)
	}
	fillAttr(ni, &out.Attr)
	return 0
}

func (n *gpixNode) Setattr(ctx context.Context, fh gofusefs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	p := n.currentPath()
	if mode, ok := in.GetMode(); ok {
		if err := n.v.Chmod(ctx, p, fs.FileMode(mode&0o7777)); err != nil {
			return toErrno(err)
		}
	}
	uid, hasUID := in.GetUID()
	gid, hasGID := in.GetGID()
	if hasUID || hasGID {
		u, g := -1, -1
		if hasUID {
			u = int(uid)
		}
		if hasGID {
			g = int(gid)
		}
		if err := n.v.Chown(ctx, p, u, g); err != nil {
			return toErrno(err)
		}
	}
	var atPtr, mtPtr *time.Time
	if at, ok := in.GetATime(); ok {
		t := at
		atPtr = &t
	}
	if mt, ok := in.GetMTime(); ok {
		t := mt
		mtPtr = &t
	}
	if atPtr != nil || mtPtr != nil {
		if err := n.v.Utimens(ctx, p, atPtr, mtPtr); err != nil {
			return toErrno(err)
		}
	}
	if sz, ok := in.GetSize(); ok {
		if h, ok := fh.(*fuseHandle); ok {
			if err := h.fh.Truncate(int64(sz)); err != nil {
				return toErrno(err)
			}
			return h.Getattr(ctx, out)
		}
		if err := n.v.Truncate(ctx, p, int64(sz)); err != nil {
			return toErrno(err)
		}
	}
	return n.Getattr(ctx, fh, out)
}

func (n *gpixNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	childPath := path.Join(n.currentPath(), name)
	ni, err := n.v.Lstat(ctx, childPath)
	if err != nil {
		return nil, toErrno(err)
	}
	fillAttr(ni, &out.Attr)
	child := &gpixNode{v: n.v, cleanPath: childPath}
	return n.NewInode(ctx, child, nodeStableAttr(ni)), 0
}

func (n *gpixNode) Readdir(ctx context.Context) (gofusefs.DirStream, syscall.Errno) {
	items, err := n.v.ReadDir(ctx, n.currentPath())
	if err != nil {
		return nil, toErrno(err)
	}
	entries := make([]fuse.DirEntry, 0, len(items))
	for _, it := range items {
		mode := uint32(syscall.S_IFREG)
		if it.FileMode&fs.ModeSymlink != 0 {
			mode = syscall.S_IFLNK
		} else if it.IsDirFlag {
			mode = syscall.S_IFDIR
		}
		entries = append(entries, fuse.DirEntry{
			Name: it.FileName,
			Ino:  it.Ino,
			Mode: mode,
		})
	}
	return gofusefs.NewListDirStream(entries), 0
}

func (n *gpixNode) Open(ctx context.Context, flags uint32) (gofusefs.FileHandle, uint32, syscall.Errno) {
	p := n.currentPath()
	fh, err := n.v.OpenFile(ctx, p, int(flags), 0o644)
	if err != nil {
		return nil, 0, toErrno(err)
	}
	var fuseFlags uint32
	if flags&uint32(os.O_WRONLY|os.O_RDWR|os.O_TRUNC|os.O_APPEND) == 0 {
		fuseFlags = fuse.FOPEN_KEEP_CACHE
	}
	return &fuseHandle{node: n, fh: fh}, fuseFlags, 0
}

func (n *gpixNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*gofusefs.Inode, gofusefs.FileHandle, uint32, syscall.Errno) {
	childPath := path.Join(n.currentPath(), name)
	perm := fs.FileMode(mode & 0o7777)
	if perm == 0 {
		perm = 0o644
	}
	fh, err := n.v.OpenFile(ctx, childPath, int(flags)|os.O_CREATE, perm)
	if err != nil {
		return nil, nil, 0, toErrno(err)
	}
	var uid, gid uint32
	if caller, ok := fuse.FromContext(ctx); ok {
		uid, gid = caller.Uid, caller.Gid
	}
	ni := &vfs.NodeInfo{
		FileName: name,
		FileSize: fh.Size(),
		FileMode: perm,
		Mtime:    time.Now(),
		Ino:      vfs.StableIno(childPath),
		Nlink:    1,
		UID:      uid,
		GID:      gid,
	}
	fillAttr(ni, &out.Attr)
	child := &gpixNode{v: n.v, cleanPath: childPath}
	inode := n.NewInode(ctx, child, nodeStableAttr(ni))
	return inode, &fuseHandle{node: child, fh: fh}, 0, 0
}

func (n *gpixNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	childPath := path.Join(n.currentPath(), name)
	perm := fs.FileMode(mode & 0o7777)
	if perm == 0 {
		perm = 0o755
	}
	if err := n.v.Mkdir(ctx, childPath, perm); err != nil {
		return nil, toErrno(err)
	}
	if caller, ok := fuse.FromContext(ctx); ok {
		_ = n.v.Chown(ctx, childPath, int(caller.Uid), int(caller.Gid))
	}
	ni, err := n.v.Lstat(ctx, childPath)
	if err != nil {
		ni = &vfs.NodeInfo{
			FileName:  name,
			FileMode:  fs.ModeDir | perm,
			Mtime:     time.Now(),
			IsDirFlag: true,
			Ino:       vfs.StableIno("dir:" + childPath),
			Nlink:     2,
		}
	}
	fillAttr(ni, &out.Attr)
	child := &gpixNode{v: n.v, cleanPath: childPath}
	return n.NewInode(ctx, child, nodeStableAttr(ni)), 0
}

func (n *gpixNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath := path.Join(n.currentPath(), name)
	if err := n.v.Remove(ctx, childPath); err != nil {
		if err.Error() == "vfs: directory not empty" {
			return syscall.ENOTEMPTY
		}
		return toErrno(err)
	}
	return 0
}

func (n *gpixNode) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath := path.Join(n.currentPath(), name)
	return toErrno(n.v.Remove(ctx, childPath))
}

func (n *gpixNode) Rename(ctx context.Context, name string, newParent gofusefs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := path.Join(n.currentPath(), name)
	np, ok := newParent.(*gpixNode)
	if !ok {
		return syscall.EINVAL
	}
	newPath := path.Join(np.currentPath(), newName)
	return toErrno(n.v.Rename(ctx, oldPath, newPath))
}

func (n *gpixNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	childPath := path.Join(n.currentPath(), name)
	var uid, gid uint32
	if caller, ok := fuse.FromContext(ctx); ok {
		uid, gid = caller.Uid, caller.Gid
	}
	ni, err := n.v.Symlink(ctx, target, childPath, uid, gid)
	if err != nil {
		return nil, toErrno(err)
	}
	fillAttr(ni, &out.Attr)
	child := &gpixNode{v: n.v, cleanPath: childPath}
	return n.NewInode(ctx, child, nodeStableAttr(ni)), 0
}

func (n *gpixNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	target, err := n.v.Readlink(ctx, n.currentPath())
	if err != nil {
		return nil, toErrno(err)
	}
	return []byte(target), 0
}

func (n *gpixNode) Link(ctx context.Context, target gofusefs.InodeEmbedder, name string, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	tn, ok := target.(*gpixNode)
	if !ok {
		return nil, syscall.EINVAL
	}
	newPath := path.Join(n.currentPath(), name)
	ni, err := n.v.Link(ctx, tn.currentPath(), newPath)
	if err != nil {
		return nil, toErrno(err)
	}
	fillAttr(ni, &out.Attr)
	return target.EmbeddedInode(), 0
}

func (n *gpixNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	qi, err := n.v.Statfs(ctx)
	if err != nil {
		return toErrno(err)
	}
	out.Bsize = qi.BlockSize
	out.Frsize = qi.BlockSize
	out.Blocks = qi.TotalBlocks
	out.Bfree = qi.FreeBlocks
	out.Bavail = qi.AvailBlocks
	out.Files = 1_000_000
	out.Ffree = 999_000
	out.NameLen = 255
	return 0
}

type fuseHandle struct {
	node *gpixNode
	fh   *vfs.FileHandle
}

var (
	_ = (gofusefs.FileReader)((*fuseHandle)(nil))
	_ = (gofusefs.FileWriter)((*fuseHandle)(nil))
	_ = (gofusefs.FileFlusher)((*fuseHandle)(nil))
	_ = (gofusefs.FileReleaser)((*fuseHandle)(nil))
	_ = (gofusefs.FileFsyncer)((*fuseHandle)(nil))
	_ = (gofusefs.FileGetattrer)((*fuseHandle)(nil))
	_ = (gofusefs.FileSetattrer)((*fuseHandle)(nil))
)

func (h *fuseHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := h.fh.ReadAt(dest, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, toErrno(err)
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (h *fuseHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := h.fh.WriteAt(data, off)
	if err != nil {
		return 0, toErrno(err)
	}
	return uint32(n), 0
}

func (h *fuseHandle) Flush(ctx context.Context) syscall.Errno {
	return toErrno(h.fh.Flush())
}

func (h *fuseHandle) Release(ctx context.Context) syscall.Errno {
	return toErrno(h.fh.Close())
}

func (h *fuseHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return toErrno(h.fh.Sync())
}

func (h *fuseHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	p := h.node.currentPath()
	ni, err := h.node.v.Lstat(ctx, p)
	if err != nil {
		st, serr := h.fh.Stat()
		if serr != nil {
			return toErrno(err)
		}
		if vni, ok := st.(*vfs.NodeInfo); ok {
			ni = vni
		} else {
			ni = &vfs.NodeInfo{
				FileName: st.Name(),
				FileSize: st.Size(),
				FileMode: st.Mode(),
				Mtime:    st.ModTime(),
				Ino:      vfs.StableIno(p),
				Nlink:    1,
			}
		}
	} else {
		ni.FileSize = h.fh.Size()
	}
	fillAttr(ni, &out.Attr)
	return 0
}

func (h *fuseHandle) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return h.node.Setattr(ctx, h, in, out)
}
