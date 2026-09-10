package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/rivetdb/rivetdb/internal/storage"
)

const DefaultMaxIndexBlockSize = 256 << 20

// ReaderOptions bounds resident index memory. Zero selects the default.
type ReaderOptions struct {
	MaxIndexBlockSize uint64
}

// Entry is one immutable internal-key/value entry. A tombstone has KindDelete.
type Entry struct {
	Key   storage.InternalKey
	Value []byte
}

type randomAccessFile interface {
	ReadAt([]byte, int64) (int, error)
	Close() error
}

// Reader provides concurrent access to one fully validated immutable SSTable.
type Reader struct {
	mu       sync.RWMutex
	file     randomAccessFile
	path     string
	size     uint64
	index    []decodedIndexEntry
	dataCRCs []uint32
	metadata Metadata
	maxIndex uint64
	closed   bool
}

// Open validates the complete table while retaining only its index and metadata.
func Open(path string, options ReaderOptions) (*Reader, error) {
	file, err := os.Open(path) //nolint:gosec // caller selects the table path
	if err != nil {
		return nil, fmt.Errorf("open SSTable %s: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, closeOnOpenFailure(file, fmt.Errorf("stat SSTable %s: %w", path, err))
	}
	if info.Size() < 0 {
		return nil, closeOnOpenFailure(file, corrupt(ErrInvalidHandle, "negative file size"))
	}
	reader, err := openReader(file, uint64(info.Size()), path, options) //nolint:gosec // negative size was rejected above
	if err != nil {
		return nil, closeOnOpenFailure(file, err)
	}
	return reader, nil
}

func openReader(file randomAccessFile, size uint64, path string, options ReaderOptions) (*Reader, error) {
	maxIndex := options.MaxIndexBlockSize
	if maxIndex == 0 {
		maxIndex = DefaultMaxIndexBlockSize
	}
	if maxIndex < BlockTrailerSize {
		return nil, ErrResourceLimit
	}
	if size < FooterSize || size > MaxTableSize {
		return nil, corrupt(ErrInvalidHandle, "invalid file size %d", size)
	}
	footerOffset := size - FooterSize
	footer := make([]byte, FooterSize)
	if err := readAtExact(file, footer, footerOffset, "footer"); err != nil {
		return nil, err
	}
	footerState, err := decodeFooter(footer, footerOffset)
	if err != nil {
		return nil, err
	}
	if footerState.index.Length > maxIndex {
		return nil, fmt.Errorf("%w: index block length %d exceeds %d", ErrResourceLimit, footerState.index.Length, maxIndex)
	}
	maximumMetadata := uint64(28 + 4*4 + 2*MaxInternalKeySize + 2*MaxUserKeySize + BlockTrailerSize)
	if footerState.metadata.Length > maximumMetadata {
		return nil, corrupt(ErrInvalidHandle, "metadata block exceeds format maximum")
	}

	r := &Reader{file: file, path: path, size: size, maxIndex: maxIndex}
	indexPayload, err := r.readBlockUnlocked(footerState.index, blockIndex, footerOffset)
	if err != nil {
		return nil, err
	}
	r.index, err = decodeIndex(indexPayload)
	if err != nil {
		return nil, err
	}
	metadataPayload, err := r.readBlockUnlocked(footerState.metadata, blockMetadata, footerOffset)
	if err != nil {
		return nil, err
	}
	diskMetadata, err := decodeMetadata(metadataPayload)
	if err != nil {
		return nil, err
	}
	if uint64(len(r.index)) != uint64(diskMetadata.dataBlockCount) {
		return nil, corrupt(ErrInvalidBlock, "index and metadata block counts differ")
	}
	if handleErr := validateDataHandles(r.index, footerState.dataEnd); handleErr != nil {
		return nil, handleErr
	}
	r.metadata, err = r.validateDataStream(diskMetadata)
	if err != nil {
		return nil, err
	}
	r.metadata.FileNumber = parseFileNumber(path)
	r.metadata.FileSize = size
	return r, nil
}

type footerState struct {
	index    BlockHandle
	metadata BlockHandle
	filter   BlockHandle
	dataEnd  uint64
}

func decodeFooter(footer []byte, footerOffset uint64) (footerState, error) {
	if len(footer) != FooterSize {
		return footerState{}, corrupt(ErrInvalidHandle, "short footer")
	}
	want := binary.LittleEndian.Uint32(footer[footerChecksum:])
	checksum := crc32.New(checksumTable)
	_, _ = checksum.Write(footer[:footerChecksum])
	_, _ = checksum.Write(footer[footerMagic:])
	if checksum.Sum32() != want {
		return footerState{}, corrupt(ErrChecksum, "footer checksum")
	}
	if binary.LittleEndian.Uint64(footer[footerMagic:]) != fileMagic {
		return footerState{}, corrupt(ErrInvalidBlock, "bad magic")
	}
	if binary.LittleEndian.Uint32(footer[footerFormatVersion:]) != FormatVersion {
		return footerState{}, fmt.Errorf("%w: version %d", ErrUnsupportedVersion, binary.LittleEndian.Uint32(footer[footerFormatVersion:]))
	}
	flags := binary.LittleEndian.Uint32(footer[footerFlags:])
	if flags&^footerFilterPresentFlag != 0 || binary.LittleEndian.Uint32(footer[footerReserved:]) != 0 {
		return footerState{}, fmt.Errorf("%w: footer flags or reserved field", ErrUnsupportedVersion)
	}
	state := footerState{
		index:    BlockHandle{Offset: binary.LittleEndian.Uint64(footer[footerIndexOffset:]), Length: binary.LittleEndian.Uint64(footer[footerIndexLength:])},
		metadata: BlockHandle{Offset: binary.LittleEndian.Uint64(footer[footerMetadataOffset:]), Length: binary.LittleEndian.Uint64(footer[footerMetadataLength:])},
		filter:   BlockHandle{Offset: binary.LittleEndian.Uint64(footer[footerFilterOffset:]), Length: binary.LittleEndian.Uint64(footer[footerFilterLength:])},
		dataEnd:  binary.LittleEndian.Uint64(footer[footerDataRegionEnd:]),
	}
	if flags&footerFilterPresentFlag != 0 {
		return footerState{}, fmt.Errorf("%w: filter blocks are not supported", ErrUnsupportedVersion)
	}
	if state.filter != (BlockHandle{}) {
		return footerState{}, corrupt(ErrInvalidHandle, "absent filter has nonzero handle")
	}
	if state.dataEnd != state.index.Offset || !handleEndsAt(state.index, state.metadata.Offset) ||
		!handleEndsAt(state.metadata, footerOffset) {
		return footerState{}, corrupt(ErrInvalidHandle, "structural regions are not contiguous")
	}
	if err := validateHandle(state.index, footerOffset, BlockTrailerSize); err != nil {
		return footerState{}, err
	}
	if err := validateHandle(state.metadata, footerOffset, BlockTrailerSize); err != nil {
		return footerState{}, err
	}
	return state, nil
}

func validateDataHandles(index []decodedIndexEntry, dataEnd uint64) error {
	next := uint64(0)
	for position, entry := range index {
		if entry.handle.Offset != next {
			return corrupt(ErrInvalidHandle, "data block %d is not physically contiguous", position)
		}
		if err := validateHandle(entry.handle, dataEnd, BlockTrailerSize); err != nil {
			return fmt.Errorf("data block %d: %w", position, err)
		}
		next = entry.handle.Offset + entry.handle.Length
	}
	if len(index) == 0 || next != dataEnd {
		return corrupt(ErrInvalidHandle, "data handles do not cover data region")
	}
	return nil
}

func validateHandle(handle BlockHandle, limit, minimumLength uint64) error {
	if handle.Length < minimumLength || handle.Offset > limit || handle.Length > limit {
		return corrupt(ErrInvalidHandle, "handle (%d,%d) outside region ending %d", handle.Offset, handle.Length, limit)
	}
	end, ok := checkedAdd(handle.Offset, handle.Length)
	if !ok || end > limit || handle.Offset > math.MaxInt64 || handle.Length > uint64(maxInt()) { //nolint:gosec // maxInt is non-negative
		return corrupt(ErrInvalidHandle, "overflowing handle (%d,%d)", handle.Offset, handle.Length)
	}
	return nil
}

func (r *Reader) validateDataStream(disk decodedMetadata) (Metadata, error) {
	var count, deletions, raw uint64
	var first, last decodedEntry
	r.dataCRCs = r.dataCRCs[:0]
	for position, indexEntry := range r.index {
		payload, err := r.readBlockUnlocked(indexEntry.handle, blockData, indexEntry.handle.Offset+indexEntry.handle.Length)
		if err != nil {
			return Metadata{}, err
		}
		block, err := decodeDataBlock(payload, indexEntry.handle)
		if err != nil {
			return Metadata{}, err
		}
		r.dataCRCs = append(r.dataCRCs, blockPayloadChecksum(payload, blockData))
		blockFirst := block.entries[0]
		blockLast := block.entries[len(block.entries)-1]
		if !bytes.Equal(blockLast.encoded, indexEntry.encoded) {
			return Metadata{}, corrupt(ErrInvalidBlock, "index key differs from data block %d last key", position)
		}
		if position != 0 && storage.CompareInternal(last.key, blockFirst.key) >= 0 {
			return Metadata{}, corrupt(ErrInvalidBlock, "cross-block order at block %d", position)
		}
		if position == 0 {
			first = blockFirst
		}
		last = blockLast
		for _, entry := range block.entries {
			if count == math.MaxUint64 {
				return Metadata{}, corrupt(ErrInvalidBlock, "entry count overflow")
			}
			count++
			if entry.key.Kind() == storage.KindDelete {
				deletions++
			}
			entryRaw, ok := checkedAdd(uint64(len(entry.encoded)), uint64(len(entry.value)))
			if !ok {
				return Metadata{}, corrupt(ErrInvalidBlock, "raw byte count overflow")
			}
			raw, ok = checkedAdd(raw, entryRaw)
			if !ok {
				return Metadata{}, corrupt(ErrInvalidBlock, "raw byte count overflow")
			}
		}
	}
	if count != disk.entryCount || deletions != disk.deletionCount || raw != disk.rawKeyValueBytes ||
		!bytes.Equal(first.encoded, disk.smallestInternal) || !bytes.Equal(last.encoded, disk.largestInternal) ||
		!bytes.Equal(first.key.UserKey(), disk.smallestUser) || !bytes.Equal(last.key.UserKey(), disk.largestUser) {
		return Metadata{}, corrupt(ErrInvalidBlock, "metadata does not match data stream")
	}
	return Metadata{
		SmallestInternal: cloneInternalKey(first.key), LargestInternal: cloneInternalKey(last.key),
		SmallestUser: bytes.Clone(disk.smallestUser), LargestUser: bytes.Clone(disk.largestUser),
		EntryCount: count, DeletionCount: deletions, RawKeyValueBytes: raw, DataBlockCount: uint32(len(r.index)), //nolint:gosec // matched an on-disk u32 count
	}, nil
}

// Metadata returns a copy of the immutable, fully cross-checked table metadata.
func (r *Reader) Metadata() Metadata {
	if r == nil {
		return Metadata{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneMetadata(r.metadata)
}

// Seek returns the first entry not less than target under CompareInternal.
func (r *Reader) Seek(target storage.InternalKey) (Entry, error) {
	entry, err := r.seekEntry(target)
	if err != nil {
		return Entry{}, err
	}
	return cloneDecodedEntry(entry), nil
}

// Get returns an exact internal-key match.
func (r *Reader) Get(target storage.InternalKey) (Entry, error) {
	entry, err := r.Seek(target)
	if err != nil {
		return Entry{}, err
	}
	if storage.CompareInternal(entry.Key, target) != 0 {
		return Entry{}, ErrNotFound
	}
	return entry, nil
}

// GetCandidate returns the newest user-key version whose sequence is at most target.
func (r *Reader) GetCandidate(userKey []byte, target uint64) (Entry, error) {
	seekKey, err := candidateSeekKey(userKey, target)
	if err != nil {
		return Entry{}, err
	}
	entry, err := r.Seek(seekKey)
	if err != nil {
		return Entry{}, err
	}
	if !bytes.Equal(entry.Key.UserKey(), userKey) {
		return Entry{}, ErrNotFound
	}
	return entry, nil
}

func candidateSeekKey(userKey []byte, target uint64) (storage.InternalKey, error) {
	if len(userKey) > MaxUserKeySize {
		return storage.InternalKey{}, ErrKeyTooLarge
	}
	key, err := storage.NewInternalKey(userKey, target, storage.KindDelete)
	if err != nil {
		return storage.InternalKey{}, fmt.Errorf("construct candidate seek key: %w", err)
	}
	return key, nil
}

func (r *Reader) seekEntry(target storage.InternalKey) (decodedEntry, error) {
	if r == nil {
		return decodedEntry{}, ErrClosed
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return decodedEntry{}, ErrClosed
	}
	blockIndex := lowerBoundIndex(r.index, target)
	if blockIndex == len(r.index) {
		return decodedEntry{}, ErrNotFound
	}
	payload, err := r.readDataPayloadUnlocked(blockIndex)
	if err != nil {
		return decodedEntry{}, err
	}
	entry, found, err := seekDataBlock(payload, target)
	if err != nil {
		return decodedEntry{}, err
	}
	if found {
		return entry, nil
	}
	blockIndex++
	if blockIndex == len(r.index) {
		return decodedEntry{}, ErrNotFound
	}
	payload, err = r.readDataPayloadUnlocked(blockIndex)
	if err != nil {
		return decodedEntry{}, err
	}
	entry, found, err = seekDataBlock(payload, target)
	if err != nil {
		return decodedEntry{}, err
	}
	if !found {
		return decodedEntry{}, ErrNotFound
	}
	return entry, nil
}

func lowerBoundIndex(index []decodedIndexEntry, target storage.InternalKey) int {
	left, right := 0, len(index)
	for left < right {
		middle := left + (right-left)/2
		if storage.CompareInternal(index[middle].key, target) < 0 {
			left = middle + 1
		} else {
			right = middle
		}
	}
	return left
}

func (block decodedDataBlock) lowerBound(target storage.InternalKey) int {
	left, right := 0, len(block.restartEntries)
	for left < right {
		middle := left + (right-left)/2
		if storage.CompareInternal(block.restartEntries[middle].key, target) < 0 {
			left = middle + 1
		} else {
			right = middle
		}
	}
	restart := left
	if restart > 0 {
		restart--
	}
	start := block.restartEntries[restart].entryIndex
	end := len(block.entries)
	if restart+1 < len(block.restartEntries) {
		end = block.restartEntries[restart+1].entryIndex + 1
	}
	for position := start; position < end; position++ {
		if storage.CompareInternal(block.entries[position].key, target) >= 0 {
			return position
		}
	}
	return len(block.entries)
}

// ValidateAll rereads the footer and every block, then revalidates every
// structural and metadata relationship against the current file bytes.
func (r *Reader) ValidateAll() error {
	if r == nil {
		return ErrClosed
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return ErrClosed
	}
	_, err := openReader(r.file, r.size, r.path, ReaderOptions{MaxIndexBlockSize: r.maxIndex})
	return err
}

func (r *Reader) loadDataBlockUnlocked(position int) (decodedDataBlock, error) {
	payload, err := r.readDataPayloadUnlocked(position)
	if err != nil {
		return decodedDataBlock{}, err
	}
	return decodeDataBlock(payload, r.index[position].handle)
}

func (r *Reader) readDataPayloadUnlocked(position int) ([]byte, error) {
	handle := r.index[position].handle
	payload, err := r.readBlockUnlocked(handle, blockData, handle.Offset+handle.Length)
	if err != nil {
		return nil, err
	}
	if len(r.dataCRCs) == len(r.index) && blockPayloadChecksum(payload, blockData) != r.dataCRCs[position] {
		return nil, corrupt(ErrChecksum, "data block %d changed after Open", position)
	}
	return payload, nil
}

func blockPayloadChecksum(payload []byte, kind blockKind) uint32 {
	checksum := crc32.New(checksumTable)
	_, _ = checksum.Write(payload)
	_, _ = checksum.Write([]byte{compressionNone, byte(kind), blockVersion})
	return checksum.Sum32()
}

func (r *Reader) readBlockUnlocked(handle BlockHandle, expected blockKind, regionEnd uint64) ([]byte, error) {
	if err := validateHandle(handle, regionEnd, BlockTrailerSize); err != nil {
		return nil, err
	}
	if expected == blockData && handle.Length > MaxDataBlockSize+BlockTrailerSize {
		return nil, corrupt(ErrInvalidHandle, "data block exceeds format maximum")
	}
	buffer := make([]byte, int(handle.Length))
	if err := readAtExact(r.file, buffer, handle.Offset, fmt.Sprintf("block kind %d", expected)); err != nil {
		return nil, err
	}
	payloadEnd := len(buffer) - BlockTrailerSize
	if buffer[payloadEnd] != compressionNone {
		return nil, fmt.Errorf("%w: compression %d", ErrUnsupportedVersion, buffer[payloadEnd])
	}
	if buffer[payloadEnd+1] != byte(expected) {
		return nil, corrupt(ErrInvalidBlock, "expected block kind %d, got %d", expected, buffer[payloadEnd+1])
	}
	if buffer[payloadEnd+2] != blockVersion {
		return nil, fmt.Errorf("%w: block version %d", ErrUnsupportedVersion, buffer[payloadEnd+2])
	}
	want := binary.LittleEndian.Uint32(buffer[payloadEnd+3:])
	if crc32.Checksum(buffer[:payloadEnd+3], checksumTable) != want {
		return nil, corrupt(ErrChecksum, "block kind %d", expected)
	}
	return buffer[:payloadEnd], nil
}

func readAtExact(file randomAccessFile, buffer []byte, offset uint64, description string) error {
	if offset > math.MaxInt64 {
		return corrupt(ErrInvalidHandle, "%s offset exceeds int64", description)
	}
	n, err := file.ReadAt(buffer, int64(offset))
	if n < 0 || n > len(buffer) {
		return fmt.Errorf("read SSTable %s returned impossible count %d: %w", description, n, io.ErrUnexpectedEOF)
	}
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("read SSTable %s at offset %d: %w", description, offset, err)
	}
	if n != len(buffer) || err != nil {
		return corrupt(ErrInvalidBlock, "short read of %s at offset %d", description, offset)
	}
	return nil
}

// Close is idempotent and waits for active operations before closing the file.
func (r *Reader) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	if err := r.file.Close(); err != nil {
		return fmt.Errorf("close SSTable %s: %w", r.path, err)
	}
	return nil
}

func (r *Reader) isClosed() bool {
	if r == nil {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.closed
}

func cloneDecodedEntry(entry decodedEntry) Entry {
	return Entry{Key: cloneInternalKey(entry.key), Value: bytes.Clone(entry.value)}
}

func handleEndsAt(handle BlockHandle, expected uint64) bool {
	end, ok := checkedAdd(handle.Offset, handle.Length)
	return ok && end == expected
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func parseFileNumber(path string) uint64 {
	base := filepath.Base(path)
	if len(base) != 16 || !strings.HasSuffix(base, ".sst") {
		return 0
	}
	number, err := strconv.ParseUint(base[:12], 10, 64)
	if err != nil || number == 0 || number > MaxFileNumber {
		return 0
	}
	return number
}

func closeOnOpenFailure(file randomAccessFile, cause error) error {
	if err := file.Close(); err != nil {
		return errors.Join(cause, fmt.Errorf("close rejected SSTable: %w", err))
	}
	return cause
}
