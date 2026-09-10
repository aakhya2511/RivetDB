package manifest

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	CurrentFileName   = "CURRENT"
	currentTempName   = "CURRENT.tmp"
	manifestPrefix    = "MANIFEST-"
	maxManifestNumber = 999_999
)

type durableFile interface {
	io.Writer
	Sync() error
	Close() error
}

type currentOps struct {
	openTemp      func(string) (durableFile, error)
	rename        func(string, string) error
	openDirectory func(string) (syncCloser, error)
	remove        func(string) error
}

type syncCloser interface {
	Sync() error
	Close() error
}

func manifestFileName(number uint64) string { return fmt.Sprintf("MANIFEST-%06d", number) }

func parseManifestName(name string) (uint64, error) {
	if len(name) != len(manifestPrefix)+6 || !strings.HasPrefix(name, manifestPrefix) {
		return 0, ErrCurrentCorrupt
	}
	digits := name[len(manifestPrefix):]
	for _, character := range digits {
		if character < '0' || character > '9' {
			return 0, ErrCurrentCorrupt
		}
	}
	number, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || number == 0 || number > maxManifestNumber {
		return 0, ErrCurrentCorrupt
	}
	return number, nil
}

func readCurrent(directory string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(directory, CurrentFileName)) //nolint:gosec // caller-selected database directory
	if err != nil {
		return 0, fmt.Errorf("read CURRENT: %w", errors.Join(ErrCurrentCorrupt, err))
	}
	if len(data) < 2 || data[len(data)-1] != '\n' || bytesCount(data, '\n') != 1 {
		return 0, ErrCurrentCorrupt
	}
	return parseManifestName(string(data[:len(data)-1]))
}

func publishCurrent(directory string, number uint64, ops currentOps) (ambiguous bool, resultErr error) {
	name := manifestFileName(number)
	if _, err := parseManifestName(name); err != nil {
		return false, err
	}
	temporary := filepath.Join(directory, currentTempName)
	final := filepath.Join(directory, CurrentFileName)
	file, err := ops.openTemp(temporary)
	if errors.Is(err, os.ErrExist) {
		return false, ErrCurrentTempExists
	}
	if err != nil {
		return false, fmt.Errorf("create CURRENT temporary file: %w", err)
	}
	safeRemove := true
	defer func() {
		if safeRemove {
			resultErr = errors.Join(resultErr, removeCurrentTemp(ops, temporary))
		}
	}()
	if _, writeErr := io.WriteString(file, name+"\n"); writeErr != nil {
		return false, errors.Join(fmt.Errorf("write CURRENT temporary file: %w", writeErr), file.Close())
	}
	if syncErr := file.Sync(); syncErr != nil {
		return false, errors.Join(fmt.Errorf("sync CURRENT temporary file: %w", syncErr), file.Close())
	}
	if closeErr := file.Close(); closeErr != nil {
		return false, fmt.Errorf("close CURRENT temporary file: %w", closeErr)
	}
	if renameErr := ops.rename(temporary, final); renameErr != nil {
		return false, fmt.Errorf("rename CURRENT: %w", renameErr)
	}
	safeRemove = false
	directoryFile, err := ops.openDirectory(directory)
	if err != nil {
		return true, fmt.Errorf("open CURRENT directory for sync: %w", err)
	}
	if err := directoryFile.Sync(); err != nil {
		return true, errors.Join(fmt.Errorf("sync CURRENT directory: %w", err), directoryFile.Close())
	}
	if err := directoryFile.Close(); err != nil {
		return true, fmt.Errorf("close synced CURRENT directory: %w", err)
	}
	return false, nil
}

func osCurrentOps() currentOps {
	return currentOps{
		openTemp: func(path string) (durableFile, error) {
			return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // database path
		},
		rename:        os.Rename,
		openDirectory: func(path string) (syncCloser, error) { return os.Open(path) }, //nolint:gosec // database path
		remove:        os.Remove,
	}
}

func removeCurrentTemp(ops currentOps, path string) error {
	if err := ops.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove CURRENT temporary file: %w", err)
	}
	return nil
}

func bytesCount(data []byte, value byte) int {
	count := 0
	for _, current := range data {
		if current == value {
			count++
		}
	}
	return count
}
