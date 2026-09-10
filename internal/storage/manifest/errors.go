// Package manifest implements the durable Manifest and immutable VersionSet.
package manifest

import "errors"

var (
	ErrInvalidOptions          = errors.New("invalid Manifest options")
	ErrInvalidEdit             = errors.New("invalid VersionEdit")
	ErrUnsupportedVersion      = errors.New("unsupported VersionEdit version or required field")
	ErrManifestCorrupt         = errors.New("corrupt Manifest")
	ErrCurrentCorrupt          = errors.New("corrupt CURRENT file")
	ErrMissingManifest         = errors.New("CURRENT Manifest is missing")
	ErrMissingLiveTable        = errors.New("live SSTable is missing")
	ErrCorruptLiveTable        = errors.New("live SSTable is corrupt or mismatched")
	ErrFileNumberCollision     = errors.New("SSTable file number collision")
	ErrFileNumberExhausted     = errors.New("SSTable file numbers exhausted")
	ErrSequenceRegression      = errors.New("last sequence regressed")
	ErrSequenceExhausted       = errors.New("storage sequence exhausted")
	ErrFrontierRegression      = errors.New("WAL replay frontier regressed")
	ErrFrontierGap             = errors.New("WAL replay frontier crosses an uninstalled gap")
	ErrFrontierSplitsBatch     = errors.New("WAL replay frontier splits an atomic batch")
	ErrWriterPoisoned          = errors.New("manifest writer is poisoned")
	ErrClosed                  = errors.New("manifest VersionSet is closed")
	ErrCurrentAmbiguous        = errors.New("CURRENT publication is ambiguous")
	ErrCurrentTempExists       = errors.New("CURRENT temporary file already exists")
	ErrManifestNumberExhausted = errors.New("manifest file numbers exhausted")
	ErrInvalidLevel            = errors.New("invalid LSM level")
	ErrDuplicateFile           = errors.New("duplicate live SSTable")
	ErrUnknownFile             = errors.New("delete references unknown live SSTable")
	ErrOverlappingLevel        = errors.New("nonzero LSM level contains overlapping tables")
	ErrMetadataMismatch        = errors.New("manifest and SSTable metadata differ")
	ErrComparatorMismatch      = errors.New("internal-key comparator identity mismatch")
)
