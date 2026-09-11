package manifest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/invariant"
	"github.com/rivetdb/rivetdb/internal/rlog"
	"github.com/rivetdb/rivetdb/internal/storage/pipeline"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/storage/wal"
)

// Options configures Manifest creation and recovery.
type Options struct {
	Directory string
	Clock     clock.Clock
	Logger    *slog.Logger
	// Mode defaults to ModeStandalone. ModeReplicated is persisted and cannot
	// open or convert a standalone database directory.
	Mode StorageMode
	// ReplicatedFrontierHook observes a replicated applied frontier after its
	// Manifest edit is durable, but before the new Version is published.
	ReplicatedFrontierHook func(uint64)
}

type manifestWriter interface {
	Append([]byte) (wal.Position, error)
	Close() error
}

// Stats is copied observability state for the metadata authority.
type Stats struct {
	ManifestNumber           uint64
	ManifestEdits            uint64
	ManifestBytes            uint64
	ManifestFsyncs           uint64
	ManifestRewrites         uint64
	RecoveryNanos            uint64
	LiveTables               uint64
	OrphanTables             uint64
	VersionGeneration        uint64
	ReplayFrontier           uint64
	HaveFrontier             bool
	Mode                     StorageMode
	ReplicatedAppliedThrough uint64
	HaveReplicatedApplied    bool
	MaxAppliedMVCC           uint64
	HaveMaxAppliedMVCC       bool
}

// Store serializes durable Manifest edits and publishes immutable Versions.
type Store struct {
	mu sync.RWMutex

	directory              string
	writer                 manifestWriter
	manifestNumber         uint64
	nextManifest           uint64
	current                *Version
	discovery              Discovery
	nextSequence           uint64
	seqExhausted           bool
	poisoned               error
	closed                 bool
	closeErr               error
	clock                  clock.Clock
	logger                 *slog.Logger
	stats                  Stats
	currentOps             currentOps
	afterDurable           func() error
	replicatedFrontierHook func(uint64)
	obsolete               []uint64
}

// Create creates the first Manifest and publishes CURRENT crash-safely.
func Create(options Options) (*Store, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	configureOptions(&options)
	if _, err := os.Lstat(filepath.Join(options.Directory, CurrentFileName)); err == nil {
		return nil, fmt.Errorf("create storage metadata: %w", ErrCurrentCorrupt)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("check existing CURRENT: %w", err)
	}
	number, err := nextManifestNumber(options.Directory)
	if err != nil {
		return nil, err
	}
	writer, err := openManifestWriter(options.Directory, number)
	if err != nil {
		return nil, err
	}
	comparator := ComparatorName
	nextFile := uint64(1)
	initial := VersionEdit{Comparator: &comparator, NextFileNumber: &nextFile}
	if options.Mode == ModeReplicated || options.Mode == ModeReplicatedMVCC {
		mode, zero := options.Mode, uint64(0)
		initial.StorageMode, initial.ReplicatedAppliedThrough = &mode, &zero
	}
	version, err := initialVersion().apply(initial)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("construct initial Version: %w", err), writer.Close())
	}
	encoded, err := EncodeVersionEdit(initial)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("encode initial VersionEdit: %w", err), writer.Close())
	}
	if _, appendErr := writer.Append(encoded); appendErr != nil {
		closeErr := writer.Close()
		return nil, errors.Join(fmt.Errorf("append initial Manifest: %w", appendErr), closeErr)
	}
	ambiguous, err := publishCurrent(options.Directory, number, osCurrentOps())
	if err != nil {
		closeErr := writer.Close()
		if ambiguous {
			err = errors.Join(ErrCurrentAmbiguous, err)
		}
		return nil, errors.Join(err, closeErr)
	}
	store := newStore(options, writer, number, version)
	store.stats.ManifestEdits = 1
	store.stats.ManifestFsyncs = 1
	store.stats.ManifestBytes = uint64(len(encoded)) //nolint:gosec // bounded edit
	store.refreshStatsLocked()
	store.logger.Info("manifest created", slog.Uint64("manifest", number))
	store.logger.Info("CURRENT switched", slog.Uint64("manifest", number))
	return store, nil
}

// Open recovers the CURRENT Manifest, validates live tables and conservatively
// advances allocation and sequence authorities before returning.
func Open(options Options) (*Store, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	configureOptions(&options)
	started := options.Clock.Now()
	number, err := readCurrent(options.Directory)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(options.Directory, manifestFileName(number))
	if _, statErr := os.Stat(path); statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) {
			return nil, fmt.Errorf("open CURRENT Manifest: %w", errors.Join(ErrMissingManifest, statErr))
		}
		return nil, fmt.Errorf("stat CURRENT Manifest: %w", statErr)
	}
	recovered, err := recoverManifest(path)
	if err != nil {
		return nil, err
	}
	if recovered.wal.TailTruncated {
		if repairErr := wal.RepairTail(path, recovered.wal); repairErr != nil {
			return nil, fmt.Errorf("repair incomplete Manifest tail: %w", repairErr)
		}
	}
	if recovered.version.mode != options.Mode {
		return nil, ErrModeMismatch
	}
	writer, err := wal.OpenWriter(path, wal.WriterOptions{Durability: wal.SyncBatch})
	if err != nil {
		return nil, fmt.Errorf("open recovered Manifest writer: %w", err)
	}
	store := newStore(options, writer, number, recovered.version)
	store.nextManifest, err = nextManifestNumber(options.Directory)
	if err != nil {
		return nil, errors.Join(err, writer.Close())
	}
	report, maxObserved, err := discoverAndValidate(options.Directory, recovered.version)
	if err != nil {
		return nil, errors.Join(err, writer.Close())
	}
	store.discovery = report
	store.stats.OrphanTables = uint64(len(report.Orphans)) //nolint:gosec // nonnegative
	for _, orphan := range report.Orphans {
		store.logger.Warn("orphan table found", slog.Uint64("file", orphan))
	}
	var recoveryEdit VersionEdit
	if maxObserved >= recovered.version.nextFileNumber {
		if maxObserved >= sstable.MaxFileNumber {
			value := maxNextFileNumber()
			recoveryEdit.NextFileNumber = &value
		} else {
			value := maxObserved + 1
			recoveryEdit.NextFileNumber = &value
		}
	}
	maximum, haveMaximum := maximumVersionSequence(recovered.version)
	if options.Mode == ModeStandalone {
		walMaximum, haveWAL, walErr := maximumWALSequence(options.Directory)
		if walErr != nil {
			return nil, errors.Join(walErr, writer.Close())
		}
		if haveWAL && (!haveMaximum || walMaximum > maximum) {
			maximum, haveMaximum = walMaximum, true
		}
	}
	if recovered.version.haveLastSequence && (!haveMaximum || recovered.version.lastSequence > maximum) {
		maximum, haveMaximum = recovered.version.lastSequence, true
	}
	if haveMaximum && (!recovered.version.haveLastSequence || maximum > recovered.version.lastSequence) {
		value := maximum
		recoveryEdit.LastSequence = &value
	}
	if recoveryEdit.NextFileNumber != nil || recoveryEdit.LastSequence != nil {
		if installErr := store.installLocked(recoveryEdit); installErr != nil {
			return nil, errors.Join(fmt.Errorf("persist conservative recovery authority: %w", installErr), writer.Close())
		}
	}
	store.nextSequence, store.seqExhausted = nextSequence(maximum, haveMaximum)
	elapsed := options.Clock.Since(started)
	if elapsed > 0 {
		store.stats.RecoveryNanos = uint64(elapsed) //nolint:gosec // positive duration
	}
	store.refreshStatsLocked()
	store.logger.Info("manifest recovered", slog.Uint64("manifest", number), slog.Uint64("version_generation", store.current.generation), slog.Uint64("last_sequence", maximum))
	store.logger.Info("next file number recovered", slog.Uint64("file", store.current.nextFileNumber))
	return store, nil
}

func newStore(options Options, writer manifestWriter, number uint64, version *Version) *Store {
	return &Store{
		directory: options.Directory, writer: writer, manifestNumber: number,
		nextManifest: number + 1, current: version, clock: options.Clock,
		logger: rlog.Component(options.Logger, "storage.manifest"), currentOps: osCurrentOps(),
		replicatedFrontierHook: options.ReplicatedFrontierHook,
		stats:                  Stats{ManifestNumber: number},
	}
}

func validateOptions(options Options) error {
	if options.Mode == 0 {
		options.Mode = ModeStandalone
	}
	if options.Directory == "" {
		return ErrInvalidOptions
	}
	if options.Mode != ModeStandalone && options.Mode != ModeReplicated && options.Mode != ModeReplicatedMVCC {
		return ErrInvalidOptions
	}
	info, err := os.Stat(options.Directory)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("%w: storage directory", ErrInvalidOptions)
	}
	return nil
}

func configureOptions(options *Options) {
	if options.Mode == 0 {
		options.Mode = ModeStandalone
	}
	if options.Clock == nil {
		options.Clock = clock.System()
	}
	if options.Logger == nil {
		options.Logger = rlog.Discard()
	}
}

func openManifestWriter(directory string, number uint64) (*wal.Writer, error) {
	if number == 0 || number > maxManifestNumber {
		return nil, ErrManifestNumberExhausted
	}
	path := filepath.Join(directory, manifestFileName(number))
	if _, err := os.Lstat(path); err == nil {
		return nil, fmt.Errorf("create Manifest %s: %w", manifestFileName(number), ErrFileNumberCollision)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("check Manifest %s: %w", manifestFileName(number), err)
	}
	writer, err := wal.OpenWriter(path, wal.WriterOptions{Durability: wal.SyncBatch})
	if err != nil {
		return nil, fmt.Errorf("create Manifest %s: %w", manifestFileName(number), err)
	}
	return writer, nil
}

func nextManifestNumber(directory string) (uint64, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return 0, fmt.Errorf("scan Manifest numbers: %w", err)
	}
	var highest uint64
	for _, entry := range entries {
		if number, parseErr := parseManifestName(entry.Name()); parseErr == nil {
			highest = max(highest, number)
		}
	}
	if highest >= maxManifestNumber {
		return 0, ErrManifestNumberExhausted
	}
	return highest + 1, nil
}

// Current returns the current immutable Version reference.
func (s *Store) Current() (*Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	return s.current, nil
}

// Discovery returns copied startup file classifications.
func (s *Store) Discovery() Discovery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Discovery{
		Live: append([]uint64(nil), s.discovery.Live...), Orphans: append([]uint64(nil), s.discovery.Orphans...),
		Temporary: append([]string(nil), s.discovery.Temporary...), Invalid: append([]string(nil), s.discovery.Invalid...),
	}
}

// RecoveredNextSequence returns the conservative next assignment and whether
// the uint64 sequence space is exhausted.
func (s *Store) RecoveredNextSequence() (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextSequence, s.seqExhausted
}

// Install validates an edit, fsyncs it, then atomically publishes its Version.
func (s *Store) Install(edit VersionEdit) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if edit.StorageMode != nil || edit.ReplicatedAppliedThrough != nil || edit.MaxAppliedMVCC != nil {
		return ErrModeMismatch
	}
	for _, table := range edit.AddedFiles {
		path := filepath.Join(s.directory, sstable.FileName(table.FileNumber))
		if err := validatePhysicalTable(path, table); err != nil {
			return fmt.Errorf("validate added SSTable before Manifest append: %w", err)
		}
	}
	return s.installLocked(edit)
}

func (s *Store) installLocked(edit VersionEdit) error {
	if s.closed {
		return ErrClosed
	}
	if s.poisoned != nil {
		return errors.Join(ErrWriterPoisoned, s.poisoned)
	}
	candidate, err := s.current.apply(edit)
	if err != nil {
		return err
	}
	encoded, err := EncodeVersionEdit(edit)
	if err != nil {
		return fmt.Errorf("encode VersionEdit: %w", err)
	}
	s.logger.Info("VersionEdit append", slog.Uint64("manifest", s.manifestNumber), slog.Uint64("version_generation", candidate.generation))
	if _, err := s.writer.Append(encoded); err != nil {
		s.poisoned = err
		return fmt.Errorf("append and sync Manifest: %w", errors.Join(ErrWriterPoisoned, err))
	}
	s.stats.ManifestEdits++
	s.stats.ManifestFsyncs++
	s.stats.ManifestBytes += uint64(len(encoded)) //nolint:gosec // bounded edit
	if edit.ReplicatedAppliedThrough != nil && s.replicatedFrontierHook != nil {
		s.replicatedFrontierHook(*edit.ReplicatedAppliedThrough)
	}
	if s.afterDurable != nil {
		if err := s.afterDurable(); err != nil {
			s.poisoned = err
			return fmt.Errorf("after durable Manifest append: %w", err)
		}
	}
	s.current = candidate
	if candidate.haveLastSequence {
		s.nextSequence, s.seqExhausted = nextSequence(candidate.lastSequence, true)
	}
	s.refreshStatsLocked()
	s.logger.Info("Version installed", slog.Uint64("manifest", s.manifestNumber), slog.Uint64("version_generation", candidate.generation), slog.Uint64("last_sequence", candidate.lastSequence))
	return nil
}

// AllocateFileNumber durably reserves and returns one collision-safe number.
func (s *Store) AllocateFileNumber(ctx context.Context) (uint64, error) {
	if ctx == nil {
		return 0, ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("before file-number reservation: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	number := s.current.nextFileNumber
	if number == 0 || number > sstable.MaxFileNumber {
		return 0, ErrFileNumberExhausted
	}
	next := number + 1
	if err := s.installLocked(VersionEdit{NextFileNumber: &next}); err != nil {
		return 0, fmt.Errorf("reserve SSTable file number %d: %w", number, err)
	}
	return number, nil
}

// InstallTable implements pipeline.TableInstaller. It persists the L0 table,
// LastSequence and the largest newly provable contiguous frontier atomically.
func (s *Store) InstallTable(ctx context.Context, installation pipeline.TableInstallation) error {
	if ctx == nil {
		return ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("before table installation: %w", err)
	}
	table, err := NewTableMetadata(0, installation.Generation, installation.SmallestSequence, installation.LargestSequence, installation.Metadata)
	if err != nil {
		return err
	}
	expectedPath := filepath.Join(s.directory, sstable.FileName(table.FileNumber))
	if filepath.Clean(installation.Path) != expectedPath {
		return errors.Join(ErrMetadataMismatch, ErrInvalidOptions)
	}
	if validateErr := validatePhysicalTable(expectedPath, table); validateErr != nil {
		return fmt.Errorf("validate pipeline SSTable before Manifest append: %w", validateErr)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	last := installation.LargestSequence
	if s.current.haveLastSequence && s.current.lastSequence > last {
		last = s.current.lastSequence
	}
	edit := VersionEdit{AddedFiles: []TableMetadata{table}, LastSequence: &last}
	if s.current.mode == ModeReplicated || s.current.mode == ModeReplicatedMVCC {
		if !installation.HaveAppliedCoverage || installation.FirstAppliedIndex != s.current.replicatedApplied+1 || installation.LastAppliedIndex < installation.FirstAppliedIndex {
			return ErrReplicatedFrontierGap
		}
		frontier := installation.LastAppliedIndex
		edit.ReplicatedAppliedThrough = &frontier
		if s.current.mode == ModeReplicatedMVCC {
			maximum := installation.MaxMVCCTimestamp
			if maximum == 0 || s.current.haveMaxAppliedMVCC && maximum < s.current.maxAppliedMVCC {
				return ErrSequenceRegression
			}
			edit.MaxAppliedMVCC = &maximum
		}
	} else {
		preliminary, applyErr := s.current.apply(edit)
		if applyErr != nil {
			return applyErr
		}
		if frontier, ok := preliminary.maximumContiguousFrontier(); ok && (!s.current.haveFrontier || frontier > s.current.frontier) {
			edit.ReplayFrontier = &frontier
		}
	}
	if err := s.installLocked(edit); err != nil {
		return fmt.Errorf("install flushed SSTable %d: %w", table.FileNumber, err)
	}
	if edit.ReplayFrontier != nil {
		s.logger.Info("replay frontier advanced", slog.Uint64("replay_frontier", *edit.ReplayFrontier))
	}
	if edit.ReplicatedAppliedThrough != nil {
		s.logger.Info("replicated applied frontier advanced", slog.Uint64("replicated_applied_through", *edit.ReplicatedAppliedThrough))
	}
	return nil
}

// InstallCompaction atomically replaces unchanged live inputs with already
// durable, validated outputs. It deliberately leaves replay-frontier metadata
// absent so structural compaction cannot manufacture WAL coverage.
func (s *Store) InstallCompaction(ctx context.Context, inputs, outputs []TableMetadata) error {
	if ctx == nil || len(inputs) == 0 || len(outputs) == 0 {
		return ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("before compaction installation: %w", err)
	}
	for _, output := range outputs {
		path := filepath.Join(s.directory, sstable.FileName(output.FileNumber))
		if err := validatePhysicalTable(path, output); err != nil {
			return fmt.Errorf("validate compaction output: %w", err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, input := range inputs {
		if !s.current.Contains(input) {
			return ErrStaleVersion
		}
	}
	edit := VersionEdit{AddedFiles: cloneTables(outputs)}
	for _, input := range inputs {
		edit.DeletedFiles = append(edit.DeletedFiles, DeletedFile{Level: input.Level, FileNumber: input.FileNumber})
	}
	if _, err := s.current.apply(edit); err != nil {
		return errors.Join(ErrStaleVersion, err)
	}
	beforeFrontier, beforeHaveFrontier := s.current.frontier, s.current.haveFrontier
	beforeReplicated, beforeHaveReplicated := s.current.replicatedApplied, s.current.haveReplicatedApplied
	if err := s.installLocked(edit); err != nil {
		return fmt.Errorf("install compaction replacement: %w", err)
	}
	invariant.Assert(s.current.frontier == beforeFrontier && s.current.haveFrontier == beforeHaveFrontier, "STORAGE-81", "compaction changed replay frontier")
	invariant.Assert(s.current.replicatedApplied == beforeReplicated && s.current.haveReplicatedApplied == beforeHaveReplicated, "REPLICA-9", "compaction changed replicated applied frontier")
	for _, input := range inputs {
		s.obsolete = append(s.obsolete, input.FileNumber)
	}
	return nil
}

// ObsoleteFiles returns logically obsolete compaction inputs. Files are not
// physically deleted while old immutable Version references may exist.
func (s *Store) ObsoleteFiles() []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]uint64(nil), s.obsolete...)
}

// Rewrite writes a one-record snapshot Manifest and atomically switches CURRENT.
func (s *Store) Rewrite() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.poisoned != nil {
		return errors.Join(ErrWriterPoisoned, s.poisoned)
	}
	if s.nextManifest == 0 || s.nextManifest > maxManifestNumber {
		return ErrManifestNumberExhausted
	}
	number := s.nextManifest
	s.logger.Info("manifest rewrite started", slog.Uint64("manifest", number))
	edit, err := s.current.snapshotEdit()
	if err != nil {
		return err
	}
	encoded, err := EncodeVersionEdit(edit)
	if err != nil {
		return fmt.Errorf("encode Manifest snapshot: %w", err)
	}
	newWriter, err := openManifestWriter(s.directory, number)
	if err != nil {
		return err
	}
	if _, appendErr := newWriter.Append(encoded); appendErr != nil {
		return errors.Join(fmt.Errorf("append Manifest snapshot: %w", appendErr), newWriter.Close())
	}
	recovered, err := recoverManifest(filepath.Join(s.directory, manifestFileName(number)))
	if err != nil {
		return errors.Join(err, newWriter.Close())
	}
	if equivalentErr := validateEquivalent(s.current, recovered.version); equivalentErr != nil {
		return errors.Join(fmt.Errorf("validate Manifest snapshot: %w", equivalentErr), newWriter.Close())
	}
	ambiguous, err := publishCurrent(s.directory, number, s.currentOps)
	if err != nil {
		closeErr := newWriter.Close()
		if ambiguous {
			s.poisoned = errors.Join(ErrCurrentAmbiguous, err)
			return errors.Join(s.poisoned, closeErr)
		}
		return errors.Join(err, closeErr)
	}
	oldWriter := s.writer
	s.writer = newWriter
	s.manifestNumber = number
	s.nextManifest = number + 1
	s.stats.ManifestNumber = number
	s.stats.ManifestRewrites++
	s.stats.ManifestBytes = uint64(len(encoded)) //nolint:gosec // bounded edit
	s.stats.ManifestEdits = 1
	s.stats.ManifestFsyncs++
	s.logger.Info("CURRENT switched", slog.Uint64("manifest", number))
	s.logger.Info("manifest rewrite completed", slog.Uint64("manifest", number), slog.Uint64("version_generation", s.current.generation))
	if err := oldWriter.Close(); err != nil {
		return fmt.Errorf("close retained old Manifest writer: %w", err)
	}
	return nil
}

// Stats returns copied metadata counters.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

func (s *Store) refreshStatsLocked() {
	s.stats.ManifestNumber = s.manifestNumber
	s.stats.LiveTables = uint64(s.current.LiveTableCount()) //nolint:gosec // nonnegative
	s.stats.VersionGeneration = s.current.generation
	s.stats.HaveFrontier = s.current.haveFrontier
	s.stats.ReplayFrontier = s.current.frontier
	s.stats.Mode = s.current.mode
	s.stats.ReplicatedAppliedThrough = s.current.replicatedApplied
	s.stats.HaveReplicatedApplied = s.current.haveReplicatedApplied
	s.stats.MaxAppliedMVCC = s.current.maxAppliedMVCC
	s.stats.HaveMaxAppliedMVCC = s.current.haveMaxAppliedMVCC
}

// Close closes the active Manifest writer and is idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.closeErr = s.writer.Close()
	return s.closeErr
}

func maximumVersionSequence(version *Version) (uint64, bool) {
	var maximum uint64
	found := false
	for _, level := range version.levels {
		for _, table := range level {
			if !found || table.LargestSequence > maximum {
				maximum, found = table.LargestSequence, true
			}
		}
	}
	return maximum, found
}

var _ pipeline.FileAllocator = (*Store)(nil)
var _ pipeline.TableInstaller = (*Store)(nil)
