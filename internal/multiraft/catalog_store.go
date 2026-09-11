package multiraft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const catalogFilename = "range-metadata"

type CatalogPublishStage uint8

const (
	CatalogTempDurable CatalogPublishStage = iota + 1
	CatalogRenamed
	CatalogDirectoryDurable
)

type CatalogPublishHook func(CatalogPublishStage) error

func LoadOrBootstrapCatalog(nodeDirectory string, bootstrap *Bootstrap, hook CatalogPublishHook) (*Catalog, error) {
	if nodeDirectory == "" {
		return nil, ErrInvalidCatalog
	}
	clusterDirectory := filepath.Join(nodeDirectory, "cluster")
	if err := os.MkdirAll(clusterDirectory, 0o750); err != nil {
		return nil, fmt.Errorf("create cluster metadata directory: %w", err)
	}
	path := filepath.Join(clusterDirectory, catalogFilename)
	encoded, err := os.ReadFile(path) //nolint:gosec // fixed filename beneath configured node root
	if err == nil {
		catalog, decodeErr := decodeCatalog(encoded)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode authoritative catalog: %w", decodeErr)
		}
		if bootstrap != nil {
			configured, configErr := NewCatalog(*bootstrap)
			if configErr != nil {
				return nil, configErr
			}
			if !catalogsEqual(catalog, configured) {
				return nil, ErrCatalogMismatch
			}
		}
		return catalog, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read authoritative catalog: %w", err)
	}
	if bootstrap == nil {
		return nil, ErrCatalogMissing
	}
	catalog, err := NewCatalog(*bootstrap)
	if err != nil {
		return nil, err
	}
	encoded, err = encodeCatalog(catalog)
	if err != nil {
		return nil, err
	}
	if err := publishCatalog(clusterDirectory, encoded, hook); err != nil {
		return nil, err
	}
	return catalog, nil
}

// ReplaceCatalog is an explicit static-administration seam used to prove the
// future generation contract. It is not a metadata consensus protocol.
func ReplaceCatalog(nodeDirectory string, expected [32]byte, next Bootstrap, hook CatalogPublishHook) (*Catalog, error) {
	current, err := LoadOrBootstrapCatalog(nodeDirectory, nil, nil)
	if err != nil {
		return nil, err
	}
	if current.Fingerprint() != expected {
		return nil, ErrCatalogMismatch
	}
	replacement, err := NewCatalog(next)
	if err != nil {
		return nil, err
	}
	if replacement.Generation() <= current.Generation() {
		return nil, fmt.Errorf("%w: catalog generation did not increase", ErrInvalidCatalog)
	}
	for _, oldDescriptor := range current.ranges {
		newDescriptor, lookupErr := replacement.LookupByID(oldDescriptor.RangeID)
		if errors.Is(lookupErr, ErrRangeNotFound) {
			continue
		}
		if lookupErr != nil {
			return nil, lookupErr
		}
		if newDescriptor.Generation < oldDescriptor.Generation ||
			!sameDescriptor(oldDescriptor, newDescriptor) && newDescriptor.Generation <= oldDescriptor.Generation {
			return nil, fmt.Errorf("%w: range %d generation did not advance", ErrInvalidCatalog, oldDescriptor.RangeID)
		}
	}
	encoded, err := encodeCatalog(replacement)
	if err != nil {
		return nil, err
	}
	if err := publishCatalog(filepath.Join(nodeDirectory, "cluster"), encoded, hook); err != nil {
		return nil, err
	}
	return replacement, nil
}

func publishCatalog(directory string, encoded []byte, hook CatalogPublishHook) (resultErr error) {
	temporary := filepath.Join(directory, ".range-metadata.tmp")
	file, openErr := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640) //nolint:gosec // authoritative metadata permissions
	if openErr != nil {
		return fmt.Errorf("create catalog temporary file: %w", openErr)
	}
	closed := false
	defer func() {
		if !closed {
			closeErr := file.Close()
			if closeErr != nil && resultErr == nil {
				resultErr = fmt.Errorf("close catalog temporary file: %w", closeErr)
			}
		}
	}()
	if _, writeErr := file.Write(encoded); writeErr != nil {
		return fmt.Errorf("write catalog temporary file: %w", writeErr)
	}
	if syncErr := file.Sync(); syncErr != nil {
		return fmt.Errorf("sync catalog temporary file: %w", syncErr)
	}
	if hook != nil {
		if hookErr := hook(CatalogTempDurable); hookErr != nil {
			return hookErr
		}
	}
	if closeErr := file.Close(); closeErr != nil {
		return fmt.Errorf("close catalog before rename: %w", closeErr)
	}
	closed = true
	if renameErr := os.Rename(temporary, filepath.Join(directory, catalogFilename)); renameErr != nil {
		return fmt.Errorf("publish catalog rename: %w", renameErr)
	}
	if hook != nil {
		if hookErr := hook(CatalogRenamed); hookErr != nil {
			return hookErr
		}
	}
	directoryFile, directoryOpenErr := os.Open(directory) //nolint:gosec // configured metadata directory
	if directoryOpenErr != nil {
		return fmt.Errorf("open catalog directory for sync: %w", directoryOpenErr)
	}
	if directorySyncErr := directoryFile.Sync(); directorySyncErr != nil {
		directoryCloseErr := directoryFile.Close()
		return errors.Join(fmt.Errorf("sync catalog directory: %w", directorySyncErr), directoryCloseErr)
	}
	if directoryCloseErr := directoryFile.Close(); directoryCloseErr != nil {
		return fmt.Errorf("close catalog directory: %w", directoryCloseErr)
	}
	if hook != nil {
		if hookErr := hook(CatalogDirectoryDurable); hookErr != nil {
			return hookErr
		}
	}
	return nil
}
