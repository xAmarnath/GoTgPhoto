package vfs

import (
	"encoding/json"
	"hash/fnv"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type DisguiseMeta struct {
	Checked       bool   `json:"checked"`
	Version       int    `json:"version"` // 0 = native media, 1 = disguised raw, 2 = disguised encrypted
	OriginalName  string `json:"originalName,omitempty"`
	PayloadSize   int64  `json:"payloadSize,omitempty"`
	PayloadOffset int64  `json:"payloadOffset,omitempty"`
}

type PosixAttr struct {
	HasMode bool   `json:"hasMode,omitempty"`
	Mode    uint32 `json:"mode,omitempty"`
	HasUID  bool   `json:"hasUid,omitempty"`
	UID     uint32 `json:"uid,omitempty"`
	HasGID  bool   `json:"hasGid,omitempty"`
	GID     uint32 `json:"gid,omitempty"`
	MtimeNs int64  `json:"mtimeNs,omitempty"`
	AtimeNs int64  `json:"atimeNs,omitempty"`
}

type VirtualEntry struct {
	MediaKey  string `json:"mediaKey"`
	SizeBytes int64  `json:"sizeBytes"`
	Ino       uint64 `json:"ino"`
	CreatedNs int64  `json:"createdNs"`
}

type SymlinkEntry struct {
	Target  string `json:"target"`
	Ino     uint64 `json:"ino"`
	UID     uint32 `json:"uid"`
	GID     uint32 `json:"gid"`
	MtimeNs int64  `json:"mtimeNs"`
}

type metaState struct {
	NextIno       uint64                  `json:"nextIno"`
	Disguise      map[string]DisguiseMeta `json:"disguise"`
	Attrs         map[string]PosixAttr    `json:"attrs"`
	Dirs          map[string]int64        `json:"dirs"` // cleanPath -> createdNs
	Entries       map[string]VirtualEntry `json:"entries"`
	Symlinks      map[string]SymlinkEntry `json:"symlinks"`
	NameOverrides map[string]string       `json:"nameOverrides"` // mediaKey -> displayName
	HiddenKeys    map[string]int64        `json:"hiddenKeys"`    // mediaKey -> hiddenAt unix sec
}

type MetaStore struct {
	mu   sync.RWMutex
	path string
	st   metaState
}

func OpenMetaStore(filePath string) (*MetaStore, error) {
	m := &MetaStore{
		path: filePath,
		st: metaState{
			NextIno:       1_000_000_000,
			Disguise:      map[string]DisguiseMeta{},
			Attrs:         map[string]PosixAttr{},
			Dirs:          map[string]int64{},
			Entries:       map[string]VirtualEntry{},
			Symlinks:      map[string]SymlinkEntry{},
			NameOverrides: map[string]string{},
			HiddenKeys:    map[string]int64{},
		},
	}
	if filePath == "" {
		return m, nil
	}
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filePath)
	if err == nil && len(data) > 0 {
		_ = json.Unmarshal(data, &m.st)
		m.ensureMaps()
	}
	return m, nil
}

func (m *MetaStore) ensureMaps() {
	if m.st.NextIno < 1_000_000_000 {
		m.st.NextIno = 1_000_000_000
	}
	if m.st.Disguise == nil {
		m.st.Disguise = map[string]DisguiseMeta{}
	}
	if m.st.Attrs == nil {
		m.st.Attrs = map[string]PosixAttr{}
	}
	if m.st.Dirs == nil {
		m.st.Dirs = map[string]int64{}
	}
	if m.st.Entries == nil {
		m.st.Entries = map[string]VirtualEntry{}
	}
	if m.st.Symlinks == nil {
		m.st.Symlinks = map[string]SymlinkEntry{}
	}
	if m.st.NameOverrides == nil {
		m.st.NameOverrides = map[string]string{}
	}
	if m.st.HiddenKeys == nil {
		m.st.HiddenKeys = map[string]int64{}
	}
}

func (m *MetaStore) saveLocked() error {
	if m.path == "" {
		return nil
	}
	data, err := json.MarshalIndent(&m.st, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}

func StableIno(seed string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(seed))
	v := h.Sum64()
	if v < 100 {
		v += 100
	}
	return v
}

func (m *MetaStore) AllocIno() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.st.NextIno++
	ino := m.st.NextIno
	_ = m.saveLocked()
	return ino
}

func (m *MetaStore) GetDisguise(mediaKey string) (DisguiseMeta, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.st.Disguise[mediaKey]
	return d, ok && d.Checked
}

func (m *MetaStore) PutDisguise(mediaKey string, d DisguiseMeta) {
	d.Checked = true
	m.mu.Lock()
	m.st.Disguise[mediaKey] = d
	_ = m.saveLocked()
	m.mu.Unlock()
}

func (m *MetaStore) GetAttr(id string) (PosixAttr, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.st.Attrs[id]
	return a, ok
}

func (m *MetaStore) UpdateAttr(id string, fn func(*PosixAttr)) {
	m.mu.Lock()
	a := m.st.Attrs[id]
	fn(&a)
	m.st.Attrs[id] = a
	_ = m.saveLocked()
	m.mu.Unlock()
}

func (m *MetaStore) NameOverride(mediaKey string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.st.NameOverrides[mediaKey]
	return v, ok && v != ""
}

func (m *MetaStore) SetNameOverride(mediaKey, newName string) {
	m.mu.Lock()
	if newName == "" {
		delete(m.st.NameOverrides, mediaKey)
	} else {
		m.st.NameOverrides[mediaKey] = newName
	}
	_ = m.saveLocked()
	m.mu.Unlock()
}

func (m *MetaStore) HideKey(mediaKey string) {
	m.mu.Lock()
	m.st.HiddenKeys[mediaKey] = time.Now().Unix()
	_ = m.saveLocked()
	m.mu.Unlock()
}

func (m *MetaStore) UnhideKey(mediaKey string) {
	m.mu.Lock()
	if _, ok := m.st.HiddenKeys[mediaKey]; ok {
		delete(m.st.HiddenKeys, mediaKey)
		_ = m.saveLocked()
	}
	m.mu.Unlock()
}

func (m *MetaStore) IsHidden(mediaKey string) bool {
	m.mu.RLock()
	ts, ok := m.st.HiddenKeys[mediaKey]
	m.mu.RUnlock()
	if !ok {
		return false
	}
	// Keep deleted media keys hidden for 10 minutes so eventual consistency never ghosts deleted items.
	if time.Now().Unix()-ts > 600 {
		m.UnhideKey(mediaKey)
		return false
	}
	return true
}

func (m *MetaStore) HasDir(clean string) (time.Time, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ns, ok := m.st.Dirs[clean]
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(0, ns), true
}

func (m *MetaStore) PutDir(clean string, mode fs.FileMode) {
	m.mu.Lock()
	now := time.Now().UnixNano()
	m.st.Dirs[clean] = now
	id := "path:" + clean
	a := m.st.Attrs[id]
	a.HasMode = true
	a.Mode = uint32(mode.Perm())
	a.MtimeNs = now
	m.st.Attrs[id] = a
	_ = m.saveLocked()
	m.mu.Unlock()
}

func (m *MetaStore) DeleteDir(clean string) {
	m.mu.Lock()
	delete(m.st.Dirs, clean)
	delete(m.st.Attrs, "path:"+clean)
	_ = m.saveLocked()
	m.mu.Unlock()
}

func (m *MetaStore) ChildDirs(parentClean string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for p := range m.st.Dirs {
		if path.Dir(p) == parentClean && p != parentClean {
			out = append(out, path.Base(p))
		}
	}
	return out
}

func (m *MetaStore) HasChildren(dirClean string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	prefix := dirClean + "/"
	if dirClean == "/" {
		prefix = "/"
	}
	for p := range m.st.Dirs {
		if p != dirClean && strings.HasPrefix(p, prefix) {
			return true
		}
	}
	for p := range m.st.Entries {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	for p := range m.st.Symlinks {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

func (m *MetaStore) GetEntry(clean string) (VirtualEntry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.st.Entries[clean]
	return e, ok
}

func (m *MetaStore) PutEntry(clean string, e VirtualEntry) {
	m.mu.Lock()
	if e.Ino == 0 {
		m.st.NextIno++
		e.Ino = m.st.NextIno
	}
	if e.CreatedNs == 0 {
		e.CreatedNs = time.Now().UnixNano()
	}
	m.st.Entries[clean] = e
	_ = m.saveLocked()
	m.mu.Unlock()
}

func (m *MetaStore) DeleteEntry(clean string) (VirtualEntry, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.st.Entries[clean]
	if !ok {
		return VirtualEntry{}, 0
	}
	delete(m.st.Entries, clean)
	delete(m.st.Attrs, "path:"+clean)
	remaining := 0
	for _, other := range m.st.Entries {
		if other.MediaKey == e.MediaKey && e.MediaKey != "" {
			remaining++
		}
	}
	_ = m.saveLocked()
	return e, remaining
}

func (m *MetaStore) RefCountIno(ino uint64, mediaKey string) uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var n uint32
	for _, e := range m.st.Entries {
		if (ino != 0 && e.Ino == ino) || (mediaKey != "" && e.MediaKey == mediaKey) {
			n++
		}
	}
	if n == 0 {
		return 1
	}
	return n
}

func (m *MetaStore) CountMediaKeyRefs(mediaKey string) int {
	if mediaKey == "" {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := 0
	for _, e := range m.st.Entries {
		if e.MediaKey == mediaKey {
			n++
		}
	}
	return n
}

func (m *MetaStore) UpdateInodeMediaKey(ino uint64, newKey string, sizeBytes int64) int {
	if ino == 0 {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	updated := 0
	for p, e := range m.st.Entries {
		if e.Ino == ino {
			e.MediaKey = newKey
			e.SizeBytes = sizeBytes
			m.st.Entries[p] = e
			updated++
		}
	}
	if updated > 0 {
		_ = m.saveLocked()
	}
	return updated
}

func (m *MetaStore) ChildEntries(parentClean string) map[string]VirtualEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]VirtualEntry{}
	for p, e := range m.st.Entries {
		if path.Dir(p) == parentClean {
			out[path.Base(p)] = e
		}
	}
	return out
}

func (m *MetaStore) GetSymlink(clean string) (SymlinkEntry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.st.Symlinks[clean]
	return s, ok
}

func (m *MetaStore) PutSymlink(clean, target string, uid, gid uint32) SymlinkEntry {
	m.mu.Lock()
	m.st.NextIno++
	s := SymlinkEntry{
		Target:  target,
		Ino:     m.st.NextIno,
		UID:     uid,
		GID:     gid,
		MtimeNs: time.Now().UnixNano(),
	}
	m.st.Symlinks[clean] = s
	_ = m.saveLocked()
	m.mu.Unlock()
	return s
}

func (m *MetaStore) DeleteSymlink(clean string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.st.Symlinks[clean]; !ok {
		return false
	}
	delete(m.st.Symlinks, clean)
	delete(m.st.Attrs, "path:"+clean)
	_ = m.saveLocked()
	return true
}

func (m *MetaStore) ChildSymlinks(parentClean string) map[string]SymlinkEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]SymlinkEntry{}
	for p, s := range m.st.Symlinks {
		if path.Dir(p) == parentClean {
			out[path.Base(p)] = s
		}
	}
	return out
}

func (m *MetaStore) RenameSubtree(oldClean, newClean string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ns, ok := m.st.Dirs[oldClean]; ok {
		delete(m.st.Dirs, oldClean)
		m.st.Dirs[newClean] = ns
	}
	prefix := oldClean + "/"
	for p, ns := range m.st.Dirs {
		if strings.HasPrefix(p, prefix) {
			delete(m.st.Dirs, p)
			m.st.Dirs[newClean+"/"+strings.TrimPrefix(p, prefix)] = ns
		}
	}
	for p, e := range m.st.Entries {
		if p == oldClean {
			delete(m.st.Entries, p)
			m.st.Entries[newClean] = e
		} else if strings.HasPrefix(p, prefix) {
			delete(m.st.Entries, p)
			m.st.Entries[newClean+"/"+strings.TrimPrefix(p, prefix)] = e
		}
	}
	for p, s := range m.st.Symlinks {
		if p == oldClean {
			delete(m.st.Symlinks, p)
			m.st.Symlinks[newClean] = s
		} else if strings.HasPrefix(p, prefix) {
			delete(m.st.Symlinks, p)
			m.st.Symlinks[newClean+"/"+strings.TrimPrefix(p, prefix)] = s
		}
	}
	for k, a := range m.st.Attrs {
		if k == "path:"+oldClean {
			delete(m.st.Attrs, k)
			m.st.Attrs["path:"+newClean] = a
		} else if strings.HasPrefix(k, "path:"+prefix) {
			delete(m.st.Attrs, k)
			m.st.Attrs["path:"+newClean+"/"+strings.TrimPrefix(k, "path:"+prefix)] = a
		}
	}
	_ = m.saveLocked()
}
