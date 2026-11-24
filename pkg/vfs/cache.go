package vfs

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"gpix/pkg/gpmc"
)

const (
	chunkSize       = 1 << 20 // 1 MiB per cached range block
	defaultMaxChunk = 160     // 160 MiB bounded RAM cache
	readAheadBlocks = 8       // sequential prefetch depth, fetched in parallel
	urlCacheTTL     = 45 * time.Minute
	maxDiskCache    = 512 << 20 // 512 MiB bounded disk decrypt cache
)

type urlEntry struct {
	url     string
	expires time.Time
}

type downloadURLCache struct {
	mu      sync.Mutex
	gp      *gpmc.Client
	entries map[string]urlEntry
}

func newDownloadURLCache(gp *gpmc.Client) *downloadURLCache {
	return &downloadURLCache{
		gp:      gp,
		entries: map[string]urlEntry{},
	}
}

func (c *downloadURLCache) Get(ctx context.Context, mediaKey string) (string, error) {
	c.mu.Lock()
	if e, ok := c.entries[mediaKey]; ok && time.Now().Before(e.expires) {
		c.mu.Unlock()
		return e.url, nil
	}
	c.mu.Unlock()

	orig, _, err := c.gp.GetDownloadURL(ctx, mediaKey)
	if err != nil {
		return "", err
	}
	if orig == "" {
		return "", fmt.Errorf("vfs: empty download url for %s", mediaKey)
	}

	c.mu.Lock()
	c.entries[mediaKey] = urlEntry{url: orig, expires: time.Now().Add(urlCacheTTL)}
	c.mu.Unlock()
	return orig, nil
}

func (c *downloadURLCache) Invalidate(mediaKey string) {
	c.mu.Lock()
	delete(c.entries, mediaKey)
	c.mu.Unlock()
}

type chunkKey struct {
	mediaKey string
	blockIdx int64
}

type chunkEntry struct {
	key  chunkKey
	data []byte
}

type inflightChunk struct {
	done chan struct{}
	data []byte
	err  error
}

type rangeChunkCache struct {
	mu       sync.Mutex
	maxItems int
	lru      *list.List
	items    map[chunkKey]*list.Element
	inflight map[chunkKey]*inflightChunk
	urls     *downloadURLCache
}

func newRangeChunkCache(urls *downloadURLCache, maxBlocks int) *rangeChunkCache {
	if maxBlocks <= 0 {
		maxBlocks = defaultMaxChunk
	}
	return &rangeChunkCache{
		maxItems: maxBlocks,
		lru:      list.New(),
		items:    map[chunkKey]*list.Element{},
		inflight: map[chunkKey]*inflightChunk{},
		urls:     urls,
	}
}

func (rc *rangeChunkCache) Len() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.lru.Len()
}

func (rc *rangeChunkCache) EvictMedia(mediaKey string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	for k, el := range rc.items {
		if k.mediaKey == mediaKey {
			rc.lru.Remove(el)
			delete(rc.items, k)
		}
	}
}

// ReadPhysicalAt reads bytes from the physical remote object starting at physOff.
func (rc *rangeChunkCache) ReadPhysicalAt(ctx context.Context, hc *http.Client, mediaKey string, p []byte, physOff int64, totalPhysLimit int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if totalPhysLimit > 0 && physOff >= totalPhysLimit {
		return 0, io.EOF
	}
	want := len(p)
	if totalPhysLimit > 0 && physOff+int64(want) > totalPhysLimit {
		want = int(totalPhysLimit - physOff)
	}

	first := physOff / chunkSize
	last := (physOff + int64(want) - 1) / chunkSize
	var limitBlk int64 = -1
	if totalPhysLimit > 0 {
		limitBlk = (totalPhysLimit - 1) / chunkSize
	}

	// Fetch every block this read touches concurrently (duplicates collapse via inflight),
	// and start prefetching the following blocks for sequential access.
	var wg sync.WaitGroup
	for b := first + 1; b <= last; b++ {
		wg.Add(1)
		go func(b int64) {
			defer wg.Done()
			_, _ = rc.getBlock(ctx, hc, mediaKey, b)
		}(b)
	}
	for b := last + 1; b <= last+readAheadBlocks; b++ {
		if limitBlk >= 0 && b > limitBlk {
			break
		}
		go func(b int64) { _, _ = rc.getBlock(context.Background(), hc, mediaKey, b) }(b)
	}
	defer wg.Wait()

	written := 0
	for written < want {
		curOff := physOff + int64(written)
		blkIdx := curOff / chunkSize
		blkOff := int(curOff % chunkSize)

		blk, err := rc.getBlock(ctx, hc, mediaKey, blkIdx)
		if err != nil {
			if written > 0 {
				return written, nil
			}
			return 0, err
		}
		if blkOff >= len(blk) {
			if written > 0 {
				return written, io.EOF
			}
			return 0, io.EOF
		}
		n := copy(p[written:want], blk[blkOff:])
		written += n
		if len(blk) < chunkSize && blkOff+n >= len(blk) {
			return written, io.EOF
		}
	}
	if written < len(p) && totalPhysLimit > 0 && physOff+int64(written) >= totalPhysLimit {
		return written, io.EOF
	}
	return written, nil
}

func (rc *rangeChunkCache) getBlock(ctx context.Context, hc *http.Client, mediaKey string, blkIdx int64) ([]byte, error) {
	key := chunkKey{mediaKey: mediaKey, blockIdx: blkIdx}

	rc.mu.Lock()
	if el, ok := rc.items[key]; ok {
		rc.lru.MoveToFront(el)
		data := el.Value.(*chunkEntry).data
		rc.mu.Unlock()
		return data, nil
	}
	if call, ok := rc.inflight[key]; ok {
		rc.mu.Unlock()
		select {
		case <-call.done:
			return call.data, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &inflightChunk{done: make(chan struct{})}
	rc.inflight[key] = call
	rc.mu.Unlock()

	fetchCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	data, err := rc.fetchBlockHTTP(fetchCtx, hc, mediaKey, blkIdx)

	rc.mu.Lock()
	delete(rc.inflight, key)
	if err == nil && len(data) > 0 {
		if el, ok := rc.items[key]; ok {
			rc.lru.MoveToFront(el)
		} else {
			el := rc.lru.PushFront(&chunkEntry{key: key, data: data})
			rc.items[key] = el
			for rc.lru.Len() > rc.maxItems {
				back := rc.lru.Back()
				if back == nil {
					break
				}
				ev := back.Value.(*chunkEntry)
				rc.lru.Remove(back)
				delete(rc.items, ev.key)
			}
		}
	}
	call.data = data
	call.err = err
	close(call.done)
	rc.mu.Unlock()

	return data, err
}

func (rc *rangeChunkCache) fetchBlockHTTP(ctx context.Context, hc *http.Client, mediaKey string, blkIdx int64) ([]byte, error) {
	start := blkIdx * chunkSize
	end := start + chunkSize - 1

	for attempt := 0; attempt < 2; attempt++ {
		dlURL, err := rc.urls.Get(ctx, mediaKey)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, dlURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		resp, err := hc.Do(req)
		if err != nil {
			return nil, err
		}
		if (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusGone) && attempt == 0 {
			resp.Body.Close()
			rc.urls.Invalidate(mediaKey)
			continue
		}
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			resp.Body.Close()
			return nil, io.EOF
		}
		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
			resp.Body.Close()
			return nil, fmt.Errorf("vfs: range GET %s status %d: %s", mediaKey, resp.StatusCode, string(body))
		}
		if resp.StatusCode == http.StatusOK && start > 0 {
			// Upstream ignored Range; discard prefix up to start.
			if _, err := io.CopyN(io.Discard, resp.Body, start); err != nil {
				resp.Body.Close()
				return nil, err
			}
		}
		buf, err := io.ReadAll(io.LimitReader(resp.Body, chunkSize))
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		return buf, nil
	}
	return nil, fmt.Errorf("vfs: failed to fetch block %d for %s", blkIdx, mediaKey)
}

type diskDecryptCache struct {
	mu       sync.Mutex
	dir      string
	maxBytes int64
}

func newDiskDecryptCache(baseTempDir string) *diskDecryptCache {
	dir := filepath.Join(baseTempDir, "gpix-decrypt-cache")
	_ = os.MkdirAll(dir, 0o700)
	return &diskDecryptCache{dir: dir, maxBytes: maxDiskCache}
}

func (dc *diskDecryptCache) Get(mediaKey string) (string, bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	p := filepath.Join(dc.dir, safeCacheName(mediaKey))
	st, err := os.Stat(p)
	if err != nil || st.IsDir() {
		return "", false
	}
	now := time.Now()
	_ = os.Chtimes(p, now, now)
	return p, true
}

func (dc *diskDecryptCache) Put(mediaKey string, r io.Reader) (string, error) {
	if err := os.MkdirAll(dc.dir, 0o700); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(dc.dir, "dec-*")
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		_ = os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return "", err
	}

	dc.mu.Lock()
	defer dc.mu.Unlock()
	dst := filepath.Join(dc.dir, safeCacheName(mediaKey))
	_ = os.Rename(tmp.Name(), dst)
	dc.evictLocked(dst)
	return dst, nil
}

func (dc *diskDecryptCache) Delete(mediaKey string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	_ = os.Remove(filepath.Join(dc.dir, safeCacheName(mediaKey)))
}

func (dc *diskDecryptCache) evictLocked(keep string) {
	entries, err := os.ReadDir(dc.dir)
	if err != nil {
		return
	}
	type item struct {
		path  string
		size  int64
		mtime time.Time
	}
	var listItems []item
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		st, err := e.Info()
		if err != nil {
			continue
		}
		full := filepath.Join(dc.dir, e.Name())
		total += st.Size()
		listItems = append(listItems, item{path: full, size: st.Size(), mtime: st.ModTime()})
	}
	if total <= dc.maxBytes {
		return
	}
	sort.Slice(listItems, func(i, j int) bool {
		return listItems[i].mtime.Before(listItems[j].mtime)
	})
	for _, it := range listItems {
		if total <= dc.maxBytes {
			break
		}
		if it.path == keep {
			continue
		}
		if os.Remove(it.path) == nil {
			total -= it.size
		}
	}
}

func safeCacheName(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			out = append(out, c)
		} else {
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return string(out)
}
