package gpmc

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	pb "gpix/pkg/gpmc/gpmcpb"
)

// Album records and per-item album membership are buried inside the same
// library-state RPC we already use for ListPage. The relevant fields, derived
// from Python gpmc.db_update_parser (commented stubs at lines 198-211 and call
// sites 257-262) and decode_message output trees:
//
//   response.1   = LibStateResponse.Body (already decoded as resp.GetBody())
//   response.1.2[]    = media items (already consumed)
//   response.1.3[]    = album/collection records (was disabled in Python)
//   response.1.12[]   = envelopes (shared-album invites — not used here)
//   response.1.9[]    = deletions (not used here)
//   response.1.6      = sync_token
//
// Album record layout (response.1.3[] each entry):
//   1               = album_media_key  (string)
//   2.5             = title            (string)
//   2.7             = total_items      (int)
//   2.8             = type             (int)
//   2.10.6.1        = start_ts_ms      (int)
//   2.10.7.1        = end_ts_ms        (int)
//   2.10.10         = last_activity_ms (int)
//   2.17.1          = cover_item_media_key (string)
//   4.2.3           = album_id         (string)
//   19.1            = sort_order       (int)
//   19.2            = is_custom_ordered (int; 1 = true)
//
// Per-media-item membership:
//   response.1.2[i].2.1.1  = collection_id (string) — the album_media_key the
//                            item belongs to. Empty when the item belongs to
//                            no user album.
//
// The proto stubs the project ships only expose field 2 (media items) and
// field 6 (sync_token) on LibStateResponse.Body; fields 3 and 12 are silently
// dropped during the proto decode. We re-decode the raw response bytes using
// the raw-proto helpers below to recover them.

type rawSyncSnapshot struct {
	syncToken   string
	albums      []Album
	memberships map[string][]string
	itemsByKey  map[string]MediaItem
}

type syncCache struct {
	at      time.Time
	snap    *rawSyncSnapshot
	invalid bool
}

const (
	syncCacheTTL = 5 * time.Minute
	syncStaleMax = 6 * time.Hour
)

// InvalidateSyncCache marks the snapshot out of date. It is still served while a background
// refresh runs: a full library walk takes many seconds and used to block every listing.
func (c *Client) InvalidateSyncCache() {
	c.syncMu.Lock()
	if c.sync != nil {
		c.sync.invalid = true
	}
	c.syncMu.Unlock()
}

func (c *Client) ListAllItems(ctx context.Context) ([]MediaItem, error) {
	snap, err := c.getSync(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]MediaItem, 0, len(snap.itemsByKey))
	for _, it := range snap.itemsByKey {
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Mtime.Equal(out[j].Mtime) {
			return out[i].Mtime.After(out[j].Mtime)
		}
		return out[i].MediaKey < out[j].MediaKey
	})
	return out, nil
}

func (c *Client) GetItem(ctx context.Context, mediaKey string) (MediaItem, bool, error) {
	snap, err := c.getSync(ctx)
	if err != nil {
		return MediaItem{}, false, err
	}
	it, ok := snap.itemsByKey[mediaKey]
	return it, ok, nil
}

func (c *Client) getSync(ctx context.Context) (*rawSyncSnapshot, error) {
	c.syncMu.Lock()
	if cur := c.sync; cur != nil {
		age := time.Since(cur.at)
		if !cur.invalid && age < syncCacheTTL {
			c.syncMu.Unlock()
			return cur.snap, nil
		}
		if age < syncStaleMax {
			c.startSyncRefreshLocked()
			c.syncMu.Unlock()
			return cur.snap, nil
		}
	}
	fl := c.startSyncRefreshLocked()
	c.syncMu.Unlock()

	select {
	case <-fl.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if fl.err != nil {
		return nil, fl.err
	}
	return fl.snap, nil
}

type syncFlight struct {
	done chan struct{}
	snap *rawSyncSnapshot
	err  error
}

// startSyncRefreshLocked joins the in-flight fetch or starts one. Caller holds syncMu.
func (c *Client) startSyncRefreshLocked() *syncFlight {
	if c.syncFlight != nil {
		return c.syncFlight
	}
	fl := &syncFlight{done: make(chan struct{})}
	c.syncFlight = fl
	go func() {
		snap, err := c.fetchSync(context.Background())
		c.syncMu.Lock()
		if err == nil {
			c.sync = &syncCache{at: time.Now(), snap: snap}
			go c.saveSyncSnapshot(snap)
		}
		c.syncFlight = nil
		c.syncMu.Unlock()
		fl.snap, fl.err = snap, err
		close(fl.done)
	}()
	return fl
}

func (c *Client) fetchSync(ctx context.Context) (*rawSyncSnapshot, error) {
	snap := &rawSyncSnapshot{
		memberships: map[string][]string{},
		itemsByKey:  map[string]MediaItem{},
	}
	cursor := ""
	emptyRun := 0
	for range maxAlbumScanPages {
		body, err := buildLibStateRequest(cursor)
		if err != nil {
			return snap, err
		}
		respBytes, err := c.doProto(ctx, "lib-state", endpointLibState, body, true, c.language)
		if err != nil {
			return snap, err
		}

		pageCount := 0
		resp := &pb.LibStateResponse{}
		if err := proto.Unmarshal(respBytes, resp); err == nil {
			decoded := decodeMediaItems(resp.GetBody().GetItems())
			pageCount += len(decoded)
			for _, it := range decoded {
				if it.MediaKey != "" {
					snap.itemsByKey[it.MediaKey] = it
				}
			}
		}

		bodyBytes, ok := findFieldBytes(respBytes, 1)
		if !ok {
			break
		}

		albumRecs := findAllFields(bodyBytes, 3)
		pageCount += len(albumRecs)
		for _, rec := range albumRecs {
			a := parseAlbumRecord(rec)
			if a.MediaKey != "" {
				snap.albums = append(snap.albums, a)
			}
		}

		for _, item := range findAllFields(bodyBytes, 2) {
			mediaKey, _ := findFieldString(item, 1)
			if mediaKey == "" {
				continue
			}
			if metaBytes, ok := findFieldBytes(item, 2); ok {
				if outerColl, ok := findFieldBytes(metaBytes, 1); ok {
					if collKey, ok := findFieldString(outerColl, 1); ok && collKey != "" {
						snap.memberships[collKey] = append(snap.memberships[collKey], mediaKey)
					}
				}
			}
		}

		next, _ := findFieldString(bodyBytes, 6)
		if next == "" || next == cursor {
			snap.syncToken = next
			break
		}
		if pageCount == 0 {
			emptyRun++
			if emptyRun >= 2 {
				snap.syncToken = next
				break
			}
		} else {
			emptyRun = 0
		}
		cursor = next
		snap.syncToken = next
	}

	return snap, nil
}

func parseAlbumRecord(buf []byte) Album {
	a := Album{Source: AlbumSourceUpstream}
	a.MediaKey, _ = findFieldString(buf, 1)
	if meta, ok := findFieldBytes(buf, 2); ok {
		a.Title, _ = findFieldString(meta, 5)
		if v, ok := findFieldVarint(meta, 7); ok {
			a.ItemCount = int(v)
		}
		if v, ok := findFieldVarint(meta, 8); ok {
			a.Type = int(v)
		}
		if range10, ok := findFieldBytes(meta, 10); ok {
			if start, ok := findFieldBytes(range10, 6); ok {
				if v, ok := findFieldVarint(start, 1); ok {
					a.StartMs = int64(v)
				}
			}
			if end, ok := findFieldBytes(range10, 7); ok {
				if v, ok := findFieldVarint(end, 1); ok {
					a.EndMs = int64(v)
				}
			}
			if v, ok := findFieldVarint(range10, 10); ok {
				a.LastActMs = int64(v)
			}
		}
		if cover, ok := findFieldBytes(meta, 17); ok {
			a.CoverMediaKey, _ = findFieldString(cover, 1)
		}
	}
	if ids, ok := findFieldBytes(buf, 4); ok {
		if sub, ok := findFieldBytes(ids, 2); ok {
			a.AlbumID, _ = findFieldString(sub, 3)
		}
	}
	if sort, ok := findFieldBytes(buf, 19); ok {
		if v, ok := findFieldVarint(sort, 1); ok {
			a.SortOrder = int(v)
		}
		if v, ok := findFieldVarint(sort, 2); ok {
			a.IsCustom = v == 1
		}
	}
	return a
}

func (c *Client) walkAlbumsFromSync(ctx context.Context) ([]Album, error) {
	snap, err := c.getSync(ctx)
	if err != nil {
		return nil, err
	}
	return snap.albums, nil
}

func (c *Client) walkAlbumMembershipFromSync(ctx context.Context, albumKey string) ([]string, []MediaItem, error) {
	snap, err := c.getSync(ctx)
	if err != nil {
		return nil, nil, err
	}
	keys := snap.memberships[albumKey]
	items := make([]MediaItem, 0, len(keys))
	for _, k := range keys {
		if it, ok := snap.itemsByKey[k]; ok {
			items = append(items, it)
		}
	}
	return keys, items, nil
}

type diskSnapshot struct {
	At          time.Time
	SyncToken   string
	Albums      []Album
	Memberships map[string][]string
	Items       []MediaItem
}

func (c *Client) snapshotPath() string {
	d, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	h := sha1.Sum([]byte(c.authData))
	return filepath.Join(d, "gpix", "sync-"+hex.EncodeToString(h[:4])+".json")
}

func (c *Client) saveSyncSnapshot(snap *rawSyncSnapshot) {
	p := c.snapshotPath()
	if p == "" {
		return
	}
	ds := diskSnapshot{At: time.Now(), SyncToken: snap.syncToken, Albums: snap.albums, Memberships: snap.memberships}
	for _, it := range snap.itemsByKey {
		ds.Items = append(ds.Items, it)
	}
	b, err := json.Marshal(ds)
	if err != nil || os.MkdirAll(filepath.Dir(p), 0o755) != nil {
		return
	}
	tmp := p + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		_ = os.Rename(tmp, p)
	}
}

// loadSyncSnapshot restores the last library snapshot so the first listing after a restart
// does not wait for a full library walk; it is served stale while a refresh runs.
func (c *Client) loadSyncSnapshot() {
	p := c.snapshotPath()
	if p == "" {
		return
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return
	}
	var ds diskSnapshot
	if json.Unmarshal(b, &ds) != nil || time.Since(ds.At) > syncStaleMax {
		return
	}
	snap := &rawSyncSnapshot{syncToken: ds.SyncToken, albums: ds.Albums, memberships: ds.Memberships, itemsByKey: make(map[string]MediaItem, len(ds.Items))}
	if snap.memberships == nil {
		snap.memberships = map[string][]string{}
	}
	for _, it := range ds.Items {
		snap.itemsByKey[it.MediaKey] = it
	}
	c.syncMu.Lock()
	c.sync = &syncCache{at: ds.At, snap: snap, invalid: true}
	c.syncMu.Unlock()
}
