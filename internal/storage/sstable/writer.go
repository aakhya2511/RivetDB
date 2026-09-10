package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/rivetdb/rivetdb/internal/invariant"
	"github.com/rivetdb/rivetdb/internal/storage"
)

// Options controls deterministic physical block construction.
type Options struct {
	BlockSize       int
	RestartInterval int
	DisableBloom    bool
}

// Metadata describes one successfully published table.
type Metadata struct {
	SmallestInternal storage.InternalKey
	LargestInternal  storage.InternalKey
	SmallestUser     []byte
	LargestUser      []byte
	FileNumber       uint64
	FileSize         uint64
	EntryCount       uint64
	DeletionCount    uint64
	RawKeyValueBytes uint64
	DataBlockCount   uint32
}

type writableFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type syncCloser interface {
	Sync() error
	Close() error
}

type publicationOps struct {
	rename        func(string, string) error
	remove        func(string) error
	openDirectory func(string) (syncCloser, error)
}

type indexEntry struct {
	lastKey []byte
	handle  BlockHandle
}

// Writer accepts one strictly ordered stream. It is intentionally not safe
// for concurrent use; the assembled engine has one serialized write path.
type Writer struct {
	file          writableFile
	publish       publicationOps
	temporaryPath string
	finalPath     string
	directory     string
	fileNumber    uint64
	blockSize     int
	block         *dataBlockBuilder
	index         []indexEntry
	offset        uint64
	entryCount    uint64
	deletionCount uint64
	rawBytes      uint64
	lastKey       storage.InternalKey
	lastEncoded   []byte
	smallestKey   storage.InternalKey
	smallestUser  []byte
	largestUser   []byte
	lastBloomUser []byte
	bloomHashes   []bloomHash
	haveBloomUser bool
	enableBloom   bool
	haveEntry     bool
	failed        error
	finished      bool
	aborted       bool
	fileClosed    bool
	metadata      Metadata
	abortErr      error
}

// OpenWriter exclusively creates the deterministic temporary table path. The
// caller must hold exclusive ownership of the database directory until Finish
// or Abort returns; the Phase 1 engine LOCK will provide that ownership.
func OpenWriter(directory string, fileNumber uint64, options Options) (*Writer, error) {
	if fileNumber == 0 || fileNumber > MaxFileNumber {
		return nil, ErrInvalidFileNumber
	}
	blockSize, restartInterval, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	finalPath := filepath.Join(directory, FileName(fileNumber))
	if _, statErr := os.Lstat(finalPath); statErr == nil {
		return nil, ErrFileExists
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect final SSTable %s: %w", finalPath, statErr)
	}
	temporaryPath := finalPath + ".tmp"
	file, err := os.OpenFile(temporaryPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // caller selects database directory
	if errors.Is(err, os.ErrExist) {
		return nil, ErrFileExists
	}
	if err != nil {
		return nil, fmt.Errorf("create temporary SSTable %s: %w", temporaryPath, err)
	}
	writer := newWriter(file, directory, temporaryPath, finalPath, fileNumber, blockSize, restartInterval, osPublication())
	writer.enableBloom = !options.DisableBloom
	return writer, nil
}

// FileName returns the sortable committed filename for fileNumber.
func FileName(fileNumber uint64) string {
	return fmt.Sprintf("%012d.sst", fileNumber)
}

func newWriter(file writableFile, directory, temporaryPath, finalPath string, fileNumber uint64, blockSize, restartInterval int, publish publicationOps) *Writer {
	return &Writer{
		file:          file,
		publish:       publish,
		directory:     directory,
		temporaryPath: temporaryPath,
		finalPath:     finalPath,
		fileNumber:    fileNumber,
		blockSize:     blockSize,
		block:         newDataBlockBuilder(restartInterval),
		enableBloom:   true,
	}
}

// Add appends one entry. Input must be strictly increasing under
// storage.CompareInternal; the writer never sorts.
func (w *Writer) Add(key storage.InternalKey, value []byte) error {
	if err := w.stateError(); err != nil {
		return err
	}
	userKey := key.UserKey()
	if len(userKey) > MaxUserKeySize {
		return ErrKeyTooLarge
	}
	encoded := key.Encode()
	if len(encoded) > MaxInternalKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}
	if key.Kind() == storage.KindDelete && len(value) != 0 {
		return ErrDeleteHasValue
	}
	entrySize, ok := checkedAdd(uint64(len(encoded)), uint64(len(value)))
	if !ok || entrySize > MaxEntrySize {
		return ErrEntryTooLarge
	}
	entrySize, ok = checkedAdd(entrySize, 30)
	if !ok || entrySize > MaxEntrySize {
		return ErrEntryTooLarge
	}

	if w.haveEntry {
		switch order := storage.CompareInternal(w.lastKey, key); {
		case order == 0:
			return ErrDuplicateKey
		case order > 0:
			return ErrOutOfOrder
		}
	}

	if !w.block.empty() && w.block.projectedSize(encoded, value) > w.blockSize {
		if err := w.flushDataBlock(); err != nil {
			return err
		}
	}
	if w.block.projectedSize(encoded, value) > MaxDataBlockSize {
		return ErrEntryTooLarge
	}
	if w.entryCount == math.MaxUint64 || key.Kind() == storage.KindDelete && w.deletionCount == math.MaxUint64 {
		return w.poison(ErrTableTooLarge)
	}
	w.block.add(encoded, value)

	w.entryCount++
	if key.Kind() == storage.KindDelete {
		w.deletionCount++
	}
	w.rawBytes, ok = checkedAdd(w.rawBytes, uint64(len(encoded)))
	if !ok {
		return w.poison(ErrTableTooLarge)
	}
	w.rawBytes, ok = checkedAdd(w.rawBytes, uint64(len(value)))
	if !ok {
		return w.poison(ErrTableTooLarge)
	}
	if !w.haveEntry {
		w.smallestKey = cloneInternalKey(key)
		w.smallestUser = bytes.Clone(userKey)
		w.haveEntry = true
	}
	w.lastKey = cloneInternalKey(key)
	w.lastEncoded = append(w.lastEncoded[:0], encoded...)
	w.largestUser = bytes.Clone(userKey)
	if !w.haveBloomUser || !bytes.Equal(w.lastBloomUser, userKey) {
		w.bloomHashes = append(w.bloomHashes, hashBloomKey(userKey))
		w.lastBloomUser = append(w.lastBloomUser[:0], userKey...)
		w.haveBloomUser = true
	}
	return nil
}

// Finish writes, fsyncs and atomically publishes the table. Successful repeat
// calls return the same metadata. An empty writer remains usable after the
// documented ErrEmptyTable result.
func (w *Writer) Finish() (Metadata, error) {
	if w.finished {
		return cloneMetadata(w.metadata), nil
	}
	if err := w.stateError(); err != nil {
		return Metadata{}, err
	}
	if !w.haveEntry {
		return Metadata{}, ErrEmptyTable
	}
	if err := w.flushDataBlock(); err != nil {
		return Metadata{}, err
	}
	dataEnd := w.offset
	indexHandle, indexErr := w.writeBlock(w.encodeIndex(), blockIndex)
	if indexErr != nil {
		return Metadata{}, indexErr
	}
	metadataHandle, metadataErr := w.writeBlock(w.encodeMetadata(), blockMetadata)
	if metadataErr != nil {
		return Metadata{}, metadataErr
	}
	var filterHandle BlockHandle
	if w.enableBloom {
		filterPayload, filterErr := encodeBloom(w.bloomHashes)
		if filterErr != nil {
			return Metadata{}, w.poison(filterErr)
		}
		filterHandle, filterErr = w.writeBlock(filterPayload, blockFilter)
		if filterErr != nil {
			return Metadata{}, filterErr
		}
	}
	footer := encodeFooter(indexHandle, metadataHandle, filterHandle, dataEnd)
	if writeErr := w.writeAll(footer[:]); writeErr != nil {
		return Metadata{}, writeErr
	}
	if syncErr := w.file.Sync(); syncErr != nil {
		return Metadata{}, w.poison(fmt.Errorf("sync temporary SSTable: %w", syncErr))
	}
	if closeErr := w.file.Close(); closeErr != nil {
		w.fileClosed = true
		return Metadata{}, w.poison(fmt.Errorf("close temporary SSTable: %w", closeErr))
	}
	w.fileClosed = true
	if renameErr := w.publish.rename(w.temporaryPath, w.finalPath); renameErr != nil {
		return Metadata{}, w.poison(fmt.Errorf("publish SSTable rename: %w", renameErr))
	}
	directory, openErr := w.publish.openDirectory(w.directory)
	if openErr != nil {
		return Metadata{}, w.poison(fmt.Errorf("open SSTable directory for sync: %w", openErr))
	}
	if err := directory.Sync(); err != nil {
		closeErr := directory.Close()
		return Metadata{}, w.poison(errors.Join(fmt.Errorf("sync SSTable directory: %w", err), closeErr))
	}
	if err := directory.Close(); err != nil {
		return Metadata{}, w.poison(fmt.Errorf("close synced SSTable directory: %w", err))
	}

	w.metadata = Metadata{
		FileNumber:       w.fileNumber,
		FileSize:         w.offset,
		EntryCount:       w.entryCount,
		DeletionCount:    w.deletionCount,
		RawKeyValueBytes: w.rawBytes,
		DataBlockCount:   uint32(len(w.index)), //nolint:gosec // flushDataBlock enforces u32 count
		SmallestInternal: cloneInternalKey(w.smallestKey),
		LargestInternal:  cloneInternalKey(w.lastKey),
		SmallestUser:     bytes.Clone(w.smallestUser),
		LargestUser:      bytes.Clone(w.largestUser),
	}
	w.finished = true
	return cloneMetadata(w.metadata), nil
}

// Abort closes and removes only the temporary path. It is idempotent and never
// removes a final path after rename.
func (w *Writer) Abort() error {
	if w.finished {
		return ErrFinished
	}
	if w.aborted {
		return w.abortErr
	}
	w.aborted = true
	var cleanup []error
	if !w.fileClosed {
		if err := w.file.Close(); err != nil {
			cleanup = append(cleanup, fmt.Errorf("close aborted SSTable: %w", err))
		}
		w.fileClosed = true
	}
	if err := w.publish.remove(w.temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		cleanup = append(cleanup, fmt.Errorf("remove aborted SSTable: %w", err))
	}
	w.abortErr = errors.Join(cleanup...)
	return w.abortErr
}

func (w *Writer) flushDataBlock() error {
	if w.block.empty() {
		return nil
	}
	if uint64(len(w.index)) >= math.MaxUint32 {
		return w.poison(ErrTooManyBlocks)
	}
	payload := w.block.finish()
	if len(payload) > MaxDataBlockSize {
		return w.poison(ErrEntryTooLarge)
	}
	handle, err := w.writeBlock(payload, blockData)
	if err != nil {
		return err
	}
	w.index = append(w.index, indexEntry{lastKey: bytes.Clone(w.lastEncoded), handle: handle})
	w.block.reset()
	return nil
}

func (w *Writer) writeBlock(payload []byte, kind blockKind) (BlockHandle, error) {
	encoded := appendBlockTrailer(payload, kind)
	length := uint64(len(encoded))
	end, ok := checkedAdd(w.offset, length)
	if !ok || end > MaxTableSize-FooterSize {
		return BlockHandle{}, w.poison(ErrTableTooLarge)
	}
	handle := BlockHandle{Offset: w.offset, Length: length}
	if err := w.writeAll(encoded); err != nil {
		return BlockHandle{}, err
	}
	return handle, nil
}

func (w *Writer) writeAll(data []byte) error {
	for len(data) != 0 {
		n, err := w.file.Write(data)
		if n < 0 || n > len(data) {
			return w.poison(fmt.Errorf("write SSTable returned impossible count %d: %w", n, io.ErrShortWrite))
		}
		w.offset += uint64(n) //nolint:gosec // n is non-negative and bounded by data length
		data = data[n:]
		if err != nil {
			return w.poison(fmt.Errorf("write SSTable at offset %d: %w", w.offset, err))
		}
		if n == 0 {
			return w.poison(fmt.Errorf("write SSTable at offset %d: %w", w.offset, io.ErrShortWrite))
		}
	}
	return nil
}

func (w *Writer) encodeIndex() []byte {
	payload := make([]byte, 0)
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(w.index))) //nolint:gosec // bounded during flush
	for _, entry := range w.index {
		payload = binary.AppendUvarint(payload, uint64(len(entry.lastKey)))
		payload = append(payload, entry.lastKey...)
		payload = binary.LittleEndian.AppendUint64(payload, entry.handle.Offset)
		payload = binary.LittleEndian.AppendUint64(payload, entry.handle.Length)
	}
	return payload
}

func (w *Writer) encodeMetadata() []byte {
	smallest := w.smallestKey.Encode()
	largest := w.lastKey.Encode()
	payload := make([]byte, 0)
	payload = binary.LittleEndian.AppendUint64(payload, w.entryCount)
	payload = binary.LittleEndian.AppendUint64(payload, w.deletionCount)
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(w.index))) //nolint:gosec // bounded during flush
	payload = binary.LittleEndian.AppendUint64(payload, w.rawBytes)
	payload = appendLengthPrefixed32(payload, smallest)
	payload = appendLengthPrefixed32(payload, largest)
	payload = appendLengthPrefixed32(payload, w.smallestUser)
	payload = appendLengthPrefixed32(payload, w.largestUser)
	return payload
}

func (w *Writer) stateError() error {
	switch {
	case w.finished:
		return ErrFinished
	case w.aborted:
		return ErrAborted
	case w.failed != nil:
		return w.failed
	default:
		return nil
	}
}

func (w *Writer) poison(cause error) error {
	if w.failed == nil {
		w.failed = errors.Join(ErrWriterFailed, cause)
	}
	return w.failed
}

func normalizeOptions(options Options) (int, int, error) {
	blockSize := options.BlockSize
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	}
	restartInterval := options.RestartInterval
	if restartInterval == 0 {
		restartInterval = DefaultRestartInterval
	}
	if blockSize < 1 || blockSize > MaxDataBlockSize || restartInterval < 1 || restartInterval > math.MaxUint16 {
		return 0, 0, ErrInvalidOptions
	}
	return blockSize, restartInterval, nil
}

func cloneInternalKey(key storage.InternalKey) storage.InternalKey {
	cloned, err := storage.NewInternalKey(key.UserKey(), key.Sequence(), key.Kind())
	invariant.Assert(err == nil, "STORAGE-31", "clone valid SSTable internal key: %v", err)
	return cloned
}

func cloneMetadata(metadata Metadata) Metadata {
	metadata.SmallestInternal = cloneInternalKey(metadata.SmallestInternal)
	metadata.LargestInternal = cloneInternalKey(metadata.LargestInternal)
	metadata.SmallestUser = bytes.Clone(metadata.SmallestUser)
	metadata.LargestUser = bytes.Clone(metadata.LargestUser)
	return metadata
}

func appendLengthPrefixed32(destination, value []byte) []byte {
	destination = binary.LittleEndian.AppendUint32(destination, uint32(len(value))) //nolint:gosec // format limits are below u32
	return append(destination, value...)
}

func checkedAdd(left, right uint64) (uint64, bool) {
	if math.MaxUint64-left < right {
		return 0, false
	}
	return left + right, true
}

func osPublication() publicationOps {
	return publicationOps{
		rename: os.Rename,
		remove: os.Remove,
		openDirectory: func(path string) (syncCloser, error) {
			return os.Open(path) //nolint:gosec // caller selects database directory
		},
	}
}
