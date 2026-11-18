package vfs

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"gpix/pkg/disguise"
	"gpix/pkg/gpmc"
)

func TestDisguisePeekMetaV1AndV2(t *testing.T) {
	payload := []byte("hello world arbitrary payload 0123456789")
	r1, _ := disguise.Wrap("archive.tar.zst", bytes.NewReader(payload), int64(len(payload)))
	wrapped1, err := io.ReadAll(r1)
	if err != nil {
		t.Fatal(err)
	}
	ver, hdr, off, err := disguise.PeekMeta(wrapped1)
	if err != nil {
		t.Fatalf("PeekMeta V1 error: %v", err)
	}
	if ver != 1 || hdr.Filename != "archive.tar.zst" || hdr.PayloadSize != int64(len(payload)) {
		t.Fatalf("unexpected V1 meta: ver=%d hdr=%+v", ver, hdr)
	}
	if !bytes.Equal(wrapped1[off:off+len(payload)], payload) {
		t.Fatalf("V1 payload offset mismatch")
	}

	r2, _, err := disguise.WrapEncrypted("secret.kdbx", bytes.NewReader(payload), "correct-horse")
	if err != nil {
		t.Fatal(err)
	}
	wrapped2, err := io.ReadAll(r2)
	if err != nil {
		t.Fatal(err)
	}
	ver2, hdr2, _, err := disguise.PeekMeta(wrapped2)
	if err != nil {
		t.Fatalf("PeekMeta V2 error: %v", err)
	}
	if ver2 != 2 || hdr2.Filename != "secret.kdbx" || hdr2.PayloadSize != int64(len(payload)) {
		t.Fatalf("unexpected V2 meta: ver=%d hdr=%+v", ver2, hdr2)
	}
}

func TestMetaStorePosixSemantics(t *testing.T) {
	tmp := t.TempDir()
	metaFile := filepath.Join(tmp, "vfs-meta.json")
	gp, err := gpmc.New("androidId=1&Token=test")
	if err != nil {
		t.Fatal(err)
	}
	v, err := New(gp, Config{TempDir: tmp, MetaFile: metaFile})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := v.Mkdir(ctx, "/projects", 0o750); err != nil {
		t.Fatalf("Mkdir /projects: %v", err)
	}
	if err := v.Mkdir(ctx, "/projects/sub", 0o700); err != nil {
		t.Fatalf("Mkdir /projects/sub: %v", err)
	}
	if err := v.Chmod(ctx, "/projects", 0o755); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	if err := v.Chown(ctx, "/projects", 1000, 1000); err != nil {
		t.Fatalf("Chown: %v", err)
	}
	mt := time.Unix(1700000000, 0)
	if err := v.Utimens(ctx, "/projects", &mt, &mt); err != nil {
		t.Fatalf("Utimens: %v", err)
	}

	if _, err := v.Symlink(ctx, "../target.txt", "/projects/sub/link.txt", 1000, 1000); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	target, err := v.Readlink(ctx, "/projects/sub/link.txt")
	if err != nil || target != "../target.txt" {
		t.Fatalf("Readlink got %q err=%v", target, err)
	}

	if err := v.Rename(ctx, "/projects", "/work"); err != nil {
		t.Fatalf("Rename subtree: %v", err)
	}

	// Re-open from persisted JSON to verify durability.
	v2, err := New(gp, Config{TempDir: tmp, MetaFile: metaFile})
	if err != nil {
		t.Fatal(err)
	}
	st, err := v2.Stat(ctx, "/work")
	if err != nil {
		t.Fatalf("Stat /work after reopen: %v", err)
	}
	if st.UID != 1000 || st.GID != 1000 || st.FileMode.Perm() != 0o755 {
		t.Fatalf("persisted attrs mismatch: %+v", st)
	}
	target2, err := v2.Readlink(ctx, "/work/sub/link.txt")
	if err != nil || target2 != "../target.txt" {
		t.Fatalf("Readlink after rename+reopen got %q err=%v", target2, err)
	}
}

func TestRangeChunkCacheSeeking(t *testing.T) {
	data := make([]byte, 3*chunkSize+12345)
	for i := range data {
		data[i] = byte((i * 31) & 0xff)
	}

	var rangeCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		if !strings.HasPrefix(rng, "bytes=") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			return
		}
		rangeCalls++
		parts := strings.Split(strings.TrimPrefix(rng, "bytes="), "-")
		start, _ := strconv.Atoi(parts[0])
		end, _ := strconv.Atoi(parts[1])
		if start >= len(data) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= len(data) {
			end = len(data) - 1
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	}))
	defer srv.Close()

	gp, _ := gpmc.New("androidId=1&Token=test")
	urls := newDownloadURLCache(gp)
	urls.entries["k1"] = urlEntry{url: srv.URL, expires: time.Now().Add(time.Hour)}
	rc := newRangeChunkCache(urls, 4)

	// Seek directly to tail (simulating MP4 moov atom read at EOF).
	tailOff := int64(len(data) - 4096)
	buf := make([]byte, 4096)
	n, err := rc.ReadPhysicalAt(context.Background(), srv.Client(), "k1", buf, tailOff, int64(len(data)))
	if err != nil && err != io.EOF {
		t.Fatalf("tail ReadPhysicalAt: %v", err)
	}
	if n != 4096 || !bytes.Equal(buf, data[tailOff:]) {
		t.Fatalf("tail read mismatch n=%d", n)
	}
	if rangeCalls != 1 {
		t.Fatalf("expected 1 HTTP Range call for tail seek, got %d", rangeCalls)
	}

	// Second read in the same 1 MiB block must hit RAM cache with 0 extra HTTP calls.
	n2, err := rc.ReadPhysicalAt(context.Background(), srv.Client(), "k1", buf[:512], tailOff+100, int64(len(data)))
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n2 != 512 || rangeCalls != 1 {
		t.Fatalf("expected cache hit (rangeCalls=1), got rangeCalls=%d", rangeCalls)
	}
}
