package vfs

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gpix/pkg/gpmc"
)

const (
	chunkSize       = 1 << 20 // 1 MiB per cached range block
	defaultMaxChunk = 160     // 160 MiB bounded RAM cache
	urlCacheTTL     = 45 * time.Minute
	maxDiskCache    = 512 << 20 // 512 MiB bounded disk decrypt cache
)

type urlEntry struct {
	url     string
	err     error
	expires time.Time
}

// missingTTL is how long a 404 from the download API is remembered, so shell probes of dead
// items fail instantly instead of costing a round trip each.
const missingTTL = 5 * time.Minute

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
		return e.url, e.err
	}
	c.mu.Unlock()

	orig, _, err := c.gp.GetDownloadURL(ctx, mediaKey)
	if err != nil {
		if isNotFound(err) {
			c.mu.Lock()
			c.entries[mediaKey] = urlEntry{err: err, expires: time.Now().Add(missingTTL)}
			c.mu.Unlock()
		}
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

// readAheadBlocks is the sequential prefetch depth, fetched in parallel (GPIX_READAHEAD).
var readAheadBlocks = func() int64 {
	if v, err := strconv.Atoi(os.Getenv("GPIX_READAHEAD")); err == nil && v >= 0 {
		return int64(v)
	}
	return 8
}()

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

	spoolMu  sync.Mutex
	spools   map[string]*spool
	noRange  map[string]bool
	rangeOK  map[string]bool
	spoolDir string
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
		spools:   map[string]*spool{},
		noRange:  map[string]bool{},
		rangeOK:  map[string]bool{},
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

	if rc.isNoRange(mediaKey) {
		return rc.spoolRead(ctx, hc, mediaKey, p, physOff, totalPhysLimit)
	}

	first := physOff / chunkSize
	if !rc.isRangeOK(mediaKey) {
		// The video host is known to ignore Range, so skip the probe request (a full round trip).
		if u, err := rc.urls.Get(ctx, mediaKey); err == nil && noRangeHost(u) {
			rc.markNoRange(mediaKey)
			return rc.spoolRead(ctx, hc, mediaKey, p, physOff, totalPhysLimit)
		}
		// Probe with a single request before fanning out: an upstream that ignores Range would
		// otherwise be hit with a full-object download per prefetched block.
		if _, err := rc.getBlock(ctx, hc, mediaKey, first); errors.Is(err, errNoRange) {
			return rc.spoolRead(ctx, hc, mediaKey, p, physOff, totalPhysLimit)
		}
	}
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
		if errors.Is(err, errNoRange) && written == 0 {
			return rc.spoolRead(ctx, hc, mediaKey, p, physOff, totalPhysLimit)
		}
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

func noRangeHost(rawURL string) bool {
	u, err := url.Parse(rawURL)
	return err == nil && strings.HasPrefix(u.Hostname(), "video-downloads.")
}

func isNotFound(err error) bool {
	var ae *gpmc.APIError
	return errors.As(err, &ae) && ae.Status == http.StatusNotFound
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

	var data []byte
	var err error
	for try := 0; try < 3; try++ {
		data, err = rc.fetchBlockHTTP(fetchCtx, hc, mediaKey, blkIdx)
		if err == nil || errors.Is(err, io.EOF) || fetchCtx.Err() != nil || isNotFound(err) || errors.Is(err, errNoRange) {
			break
		}
		time.Sleep(time.Duration(try+1) * 500 * time.Millisecond)
	}

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
		t0 := time.Now()
		if os.Getenv("GPIX_TRACE") != "" && blkIdx == 300 {
			fmt.Fprintf(os.Stderr, "URL %s\n", dlURL)
		}
		resp, err := hc.Do(req)
		if os.Getenv("GPIX_TRACE") != "" {
			code := 0
			if resp != nil {
				code = resp.StatusCode
			}
			fmt.Fprintf(os.Stderr, "FETCH blk %d status %d hdr %v err %v\n", blkIdx, code, time.Since(t0).Round(time.Millisecond), err)
		}
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
		if resp.StatusCode == http.StatusPartialContent {
			rc.spoolMu.Lock()
			rc.rangeOK[mediaKey] = true
			rc.spoolMu.Unlock()
		}
		if resp.StatusCode == http.StatusOK && resp.ContentLength > chunkSize {
			// Upstream (video-downloads.googleusercontent.com) ignores Range and streams the
			// whole object; stop fetching blocks and switch this item to a sequential spool.
			rc.markNoRange(mediaKey)
			// resp is bound to this fetch's context, which is cancelled on return, so the spool
			// opens its own request instead of adopting it.
			resp.Body.Close()
			_, _ = rc.newSpool(hc, mediaKey, nil)
			return nil, errNoRange
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

var errNoRange = errors.New("vfs: upstream does not support range requests")

func (rc *rangeChunkCache) isNoRange(mediaKey string) bool {
	rc.spoolMu.Lock()
	defer rc.spoolMu.Unlock()
	return rc.noRange[mediaKey]
}

func (rc *rangeChunkCache) isRangeOK(mediaKey string) bool {
	rc.spoolMu.Lock()
	defer rc.spoolMu.Unlock()
	return rc.rangeOK[mediaKey]
}

func (rc *rangeChunkCache) markNoRange(mediaKey string) {
	rc.spoolMu.Lock()
	rc.noRange[mediaKey] = true
	rc.spoolMu.Unlock()
}

// spool is a progressive on-disk copy of a remote object that cannot be fetched by range:
// one sequential download writes the file while readers wait for the bytes they need.
type spool struct {
	mu         sync.Mutex
	cond       *sync.Cond
	f          *os.File
	path       string
	downloaded int64
	done       bool
	err        error
	lastUse    time.Time
}

const maxSpoolBytes = 2 << 30

func (rc *rangeChunkCache) getSpool(hc *http.Client, mediaKey string) (*spool, error) {
	return rc.newSpool(hc, mediaKey, nil)
}

// newSpool returns the item's spool, creating it if needed. A non-nil first is an already open
// full-object response (from the range probe) that the download continues from.
func (rc *rangeChunkCache) newSpool(hc *http.Client, mediaKey string, first *http.Response) (*spool, error) {
	rc.spoolMu.Lock()
	defer rc.spoolMu.Unlock()
	if sp, ok := rc.spools[mediaKey]; ok {
		if first != nil {
			first.Body.Close()
		}
		return sp, nil
	}
	dir := rc.spoolDirOrDefault()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		if first != nil {
			first.Body.Close()
		}
		return nil, err
	}
	f, err := os.CreateTemp(dir, "spool-*")
	if err != nil {
		if first != nil {
			first.Body.Close()
		}
		return nil, err
	}
	sp := &spool{f: f, path: f.Name(), lastUse: time.Now()}
	sp.cond = sync.NewCond(&sp.mu)
	rc.spools[mediaKey] = sp
	rc.evictSpoolsLocked(mediaKey)
	go rc.runSpool(hc, mediaKey, sp, first)
	return sp, nil
}

// evictSpoolsLocked drops the least recently used finished spools beyond maxSpoolBytes.
func (rc *rangeChunkCache) evictSpoolsLocked(keep string) {
	type ent struct {
		key  string
		sp   *spool
		size int64
		at   time.Time
	}
	var all []ent
	var total int64
	for k, sp := range rc.spools {
		sp.mu.Lock()
		e := ent{k, sp, sp.downloaded, sp.lastUse}
		fin := sp.done
		sp.mu.Unlock()
		total += e.size
		if fin && k != keep {
			all = append(all, e)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.Before(all[j].at) })
	for _, e := range all {
		if total <= maxSpoolBytes {
			break
		}
		delete(rc.spools, e.key)
		_ = e.sp.f.Close()
		_ = os.Remove(e.sp.path)
		total -= e.size
	}
}

func (rc *rangeChunkCache) dropSpool(mediaKey string, sp *spool) {
	rc.spoolMu.Lock()
	if rc.spools[mediaKey] == sp {
		delete(rc.spools, mediaKey)
	}
	rc.spoolMu.Unlock()
	_ = sp.f.Close()
	_ = os.Remove(sp.path)
}

func (rc *rangeChunkCache) runSpool(hc *http.Client, mediaKey string, sp *spool, first *http.Response) {
	fail := func(err error) {
		sp.mu.Lock()
		sp.err = err
		sp.cond.Broadcast()
		sp.mu.Unlock()
		// Removed from the map so the next read starts a fresh download.
		rc.spoolMu.Lock()
		if rc.spools[mediaKey] == sp {
			delete(rc.spools, mediaKey)
		}
		rc.spoolMu.Unlock()
	}
	ctx := context.Background()
	for attempt := 0; attempt < 2; attempt++ {
		var resp *http.Response
		if first != nil {
			resp, first = first, nil
		} else {
			dlURL, err := rc.urls.Get(ctx, mediaKey)
			if err != nil {
				fail(err)
				return
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, dlURL, nil)
			if err != nil {
				fail(err)
				return
			}
			resp, err = hc.Do(req)
			if err != nil {
				fail(err)
				return
			}
		}
		if (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusGone) && attempt == 0 {
			resp.Body.Close()
			rc.urls.Invalidate(mediaKey)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			fail(fmt.Errorf("vfs: spool GET %s status %d", mediaKey, resp.StatusCode))
			return
		}
		buf := make([]byte, 512<<10)
		for {
			n, rerr := resp.Body.Read(buf)
			if n > 0 {
				if _, werr := sp.f.WriteAt(buf[:n], sp.downloadedNow()); werr != nil {
					resp.Body.Close()
					fail(werr)
					return
				}
				sp.mu.Lock()
				sp.downloaded += int64(n)
				sp.cond.Broadcast()
				sp.mu.Unlock()
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				resp.Body.Close()
				fail(rerr)
				return
			}
		}
		resp.Body.Close()
		sp.mu.Lock()
		sp.done = true
		sp.cond.Broadcast()
		sp.mu.Unlock()
		return
	}
	fail(fmt.Errorf("vfs: spool download failed for %s", mediaKey))
}

func (sp *spool) downloadedNow() int64 {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.downloaded
}

func (rc *rangeChunkCache) spoolRead(ctx context.Context, hc *http.Client, mediaKey string, p []byte, physOff, limit int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if limit > 0 && physOff >= limit {
		return 0, io.EOF
	}
	want := int64(len(p))
	if limit > 0 && physOff+want > limit {
		want = limit - physOff
	}
	sp, err := rc.getSpool(hc, mediaKey)
	if err != nil {
		return 0, err
	}
	stop := context.AfterFunc(ctx, func() {
		sp.mu.Lock()
		sp.cond.Broadcast()
		sp.mu.Unlock()
	})
	defer stop()

	need := physOff + want
	sp.mu.Lock()
	sp.lastUse = time.Now()
	for sp.downloaded < need && !sp.done && sp.err == nil && ctx.Err() == nil {
		sp.cond.Wait()
	}
	avail, done, serr := sp.downloaded, sp.done, sp.err
	sp.mu.Unlock()
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if physOff >= avail {
		if serr != nil {
			return 0, serr
		}
		return 0, io.EOF
	}
	n := min(want, avail-physOff)
	got, rerr := sp.f.ReadAt(p[:n], physOff)
	if rerr != nil && !errors.Is(rerr, io.EOF) {
		return got, rerr
	}
	if int64(got) < int64(len(p)) && done && physOff+int64(got) >= avail {
		return got, io.EOF
	}
	return got, nil
}

func (rc *rangeChunkCache) spoolDirOrDefault() string {
	if rc.spoolDir != "" {
		return rc.spoolDir
	}
	return filepath.Join(os.TempDir(), "gpix-spool")
}
