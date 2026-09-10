package engine

import (
	"container/list"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/rivetdb/rivetdb/internal/invariant"
	"github.com/rivetdb/rivetdb/internal/storage/manifest"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

type tableCacheStats struct {
	Hits, Misses, Opens, Evictions uint64
	CachedReaders, ActiveLeases    uint64
}

type tableCache struct {
	mu        sync.Mutex
	directory string
	capacity  int
	entries   map[uint64]*tableCacheEntry
	lru       list.List
	closed    bool
	hits      uint64
	misses    uint64
	opens     uint64
	evictions uint64
	active    uint64
}

type tableCacheEntry struct {
	fileNumber uint64
	reader     *sstable.Reader
	references uint64
	element    *list.Element
	obsolete   bool
}

type tableLease struct {
	cache  *tableCache
	entry  *tableCacheEntry
	Reader *sstable.Reader
	once   sync.Once
	err    error
}

func newTableCache(directory string, capacity int) *tableCache {
	return &tableCache{directory: directory, capacity: capacity, entries: make(map[uint64]*tableCacheEntry)}
}

func (c *tableCache) acquire(table manifest.TableMetadata) (*tableLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if entry, ok := c.entries[table.FileNumber]; ok && !entry.obsolete {
		if !metadataMatches(table, entry.reader.Metadata()) {
			return nil, errors.Join(ErrCorruption, manifest.ErrMetadataMismatch)
		}
		entry.references++
		c.active++
		c.hits++
		c.lru.MoveToFront(entry.element)
		return &tableLease{cache: c, entry: entry, Reader: entry.reader}, nil
	}

	c.misses++
	for len(c.entries) >= c.capacity {
		evicted, evictErr := c.evictOneIdleLocked()
		if evictErr != nil {
			return nil, evictErr
		}
		if !evicted {
			break
		}
	}
	reader, err := sstable.Open(filepath.Join(c.directory, sstable.FileName(table.FileNumber)), sstable.ReaderOptions{})
	if err != nil {
		return nil, fmt.Errorf("open table %d for cache: %w", table.FileNumber, err)
	}
	c.opens++
	if !metadataMatches(table, reader.Metadata()) {
		return nil, errors.Join(ErrCorruption, manifest.ErrMetadataMismatch, reader.Close())
	}
	c.active++
	if len(c.entries) >= c.capacity {
		// Every retained entry is borrowed. This reader is deliberately
		// uncached and closes with its lease, keeping retained capacity strict.
		return &tableLease{cache: c, Reader: reader}, nil
	}
	entry := &tableCacheEntry{fileNumber: table.FileNumber, reader: reader, references: 1}
	entry.element = c.lru.PushFront(entry)
	c.entries[entry.fileNumber] = entry
	return &tableLease{cache: c, entry: entry, Reader: reader}, nil
}

func (c *tableCache) evictOneIdleLocked() (bool, error) {
	for element := c.lru.Back(); element != nil; element = element.Prev() {
		entry, ok := element.Value.(*tableCacheEntry)
		invariant.Assert(ok, "STORAGE-113", "table-cache LRU contains %T", element.Value)
		if entry.references != 0 {
			continue
		}
		return true, c.removeLocked(entry)
	}
	return false, nil
}

func (c *tableCache) removeLocked(entry *tableCacheEntry) error {
	delete(c.entries, entry.fileNumber)
	c.lru.Remove(entry.element)
	c.evictions++
	if err := entry.reader.Close(); err != nil {
		return fmt.Errorf("close evicted table %d: %w", entry.fileNumber, err)
	}
	return nil
}

func (lease *tableLease) Release() error {
	if lease == nil {
		return nil
	}
	lease.once.Do(func() {
		lease.err = lease.cache.release(lease.entry, lease.Reader)
	})
	return lease.err
}

func (c *tableCache) release(entry *tableCacheEntry, reader *sstable.Reader) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
	if entry == nil {
		if err := reader.Close(); err != nil {
			return fmt.Errorf("close uncached table reader: %w", err)
		}
		return nil
	}
	entry.references--
	if entry.references == 0 && (entry.obsolete || c.closed) {
		delete(c.entries, entry.fileNumber)
		c.lru.Remove(entry.element)
		c.evictions++
		if err := entry.reader.Close(); err != nil {
			return fmt.Errorf("close released table %d: %w", entry.fileNumber, err)
		}
		return nil
	}
	return nil
}

// evict removes an idle identity before physical deletion. A borrowed reader
// is marked obsolete and retained until its last lease is released.
func (c *tableCache) evict(fileNumber uint64) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[fileNumber]
	if !ok {
		return true, nil
	}
	entry.obsolete = true
	if entry.references != 0 {
		return false, nil
	}
	delete(c.entries, fileNumber)
	c.lru.Remove(entry.element)
	c.evictions++
	if err := entry.reader.Close(); err != nil {
		return false, fmt.Errorf("close cached table %d: %w", fileNumber, err)
	}
	return true, nil
}

func (c *tableCache) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var result error
	for _, entry := range c.entries {
		entry.obsolete = true
		if entry.references == 0 {
			delete(c.entries, entry.fileNumber)
			c.lru.Remove(entry.element)
			c.evictions++
			result = errors.Join(result, entry.reader.Close())
		}
	}
	return result
}

func (c *tableCache) stats() tableCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return tableCacheStats{Hits: c.hits, Misses: c.misses, Opens: c.opens, Evictions: c.evictions, CachedReaders: uint64(len(c.entries)), ActiveLeases: c.active} //nolint:gosec // nonnegative bounded counts
}
