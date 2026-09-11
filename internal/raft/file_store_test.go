package raft

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/rivetdb/rivetdb/internal/testutil"
)

func samplePersistentState() PersistentState {
	return PersistentState{
		HardState: HardState{Term: 7, VotedFor: 3},
		Snapshot:  Snapshot{Index: 2, Term: 4, Data: []byte("snapshot")},
		Entries: []Entry{
			{Index: 3, Term: 5, Type: EntryCommand, Command: []byte("alpha")},
			{Index: 4, Term: 7, Type: EntryNoOp},
		},
	}
}

func TestFileStoreReopenExactAndOwnsBytes(t *testing.T) {
	directory := t.TempDir()
	store, openErr := OpenFileStore(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	state := samplePersistentState()
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	state.Entries[0].Command[0] = 'X'
	state.Snapshot.Data[0] = 'X'
	reopened, err := OpenFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, samplePersistentState()) {
		t.Fatalf("state=%+v", got)
	}
	got.Entries[0].Command[0] = 'Y'
	again, err := reopened.Load()
	if err != nil || reflect.DeepEqual(got, again) {
		t.Fatalf("Load exposed storage: again=%+v err=%v", again, err)
	}
}

func TestFileStoreRejectsEveryTruncationAndCorruption(t *testing.T) {
	state := samplePersistentState()
	encoded, err := encodeFileState(state)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, fileStoreName)
	store, openErr := OpenFileStore(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	for offset := range len(encoded) {
		if err := os.WriteFile(path, encoded[:offset], 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Load(); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("truncation %d error=%v", offset, err)
		}
	}
	for offset := range encoded {
		corrupt := bytes.Clone(encoded)
		corrupt[offset] ^= 0x80
		if err := os.WriteFile(path, corrupt, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Load(); !errors.Is(err, ErrCorruptStore) && !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("corruption %d error=%v", offset, err)
		}
	}
}

func TestFileStoreRejectsChecksumValidSemanticCorruption(t *testing.T) {
	encoded, err := encodeFileState(samplePersistentState())
	if err != nil {
		t.Fatal(err)
	}
	// First entry index is after hard state, snapshot metadata/data and count.
	entryOffset := fileStoreHeaderSize + 8*4 + 4 + len("snapshot") + 4
	binaryPutU64(encoded[entryOffset:], 99)
	binaryPutU32(encoded[len(encoded)-4:], checksum(encoded[:len(encoded)-4]))
	if _, err := decodeFileState(encoded); !errors.Is(err, ErrCorruptStore) || !errors.Is(err, ErrInvalidState) {
		t.Fatalf("semantic corruption error=%v", err)
	}
}

func binaryPutU64(target []byte, value uint64) {
	for index := range 8 {
		target[index] = byte(value >> (8 * index))
	}
}

func binaryPutU32(target []byte, value uint32) {
	for index := range 4 {
		target[index] = byte(value >> (8 * index))
	}
}

func checksum(data []byte) uint32 {
	return crc32.Checksum(data, raftCRCTable)
}

type scriptedFile struct {
	bytes.Buffer
	events   *[]string
	name     string
	writeErr error
	syncErr  error
	closeErr error
}

func (f *scriptedFile) Read(target []byte) (int, error) { return f.Buffer.Read(target) }

func (f *scriptedFile) Write(data []byte) (int, error) {
	*f.events = append(*f.events, f.name+":write")
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.Buffer.Write(data)
}

func (f *scriptedFile) Sync() error {
	*f.events = append(*f.events, f.name+":sync")
	return f.syncErr
}

func (f *scriptedFile) Close() error {
	*f.events = append(*f.events, f.name+":close")
	return f.closeErr
}

func TestFileStorePublicationOrderingAndFailures(t *testing.T) {
	for _, test := range []struct {
		name       string
		openFile   error
		writeFile  error
		syncFile   error
		closeFile  error
		renameFile error
		openDir    error
		syncDir    error
		closeDir   error
		wantError  bool
		want       []string
	}{
		{name: "success", want: []string{"file:write", "file:sync", "file:close", "rename", "dir:sync", "dir:close"}},
		{name: "open file", openFile: os.ErrPermission, wantError: true},
		{name: "write file", writeFile: io.ErrShortWrite, wantError: true, want: []string{"file:write", "file:close"}},
		{name: "file sync", syncFile: io.ErrUnexpectedEOF, wantError: true, want: []string{"file:write", "file:sync", "file:close"}},
		{name: "file close", closeFile: io.ErrUnexpectedEOF, wantError: true, want: []string{"file:write", "file:sync", "file:close"}},
		{name: "rename", renameFile: os.ErrPermission, wantError: true, want: []string{"file:write", "file:sync", "file:close", "rename"}},
		{name: "open directory", openDir: os.ErrPermission, wantError: true, want: []string{"file:write", "file:sync", "file:close", "rename"}},
		{name: "directory sync", syncDir: io.ErrUnexpectedEOF, wantError: true, want: []string{"file:write", "file:sync", "file:close", "rename", "dir:sync", "dir:close"}},
		{name: "directory close", closeDir: io.ErrUnexpectedEOF, wantError: true, want: []string{"file:write", "file:sync", "file:close", "rename", "dir:sync", "dir:close"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var events []string
			file := &scriptedFile{
				events: &events, name: "file", writeErr: test.writeFile,
				syncErr: test.syncFile, closeErr: test.closeFile,
			}
			directory := &scriptedFile{
				events: &events, name: "dir", syncErr: test.syncDir, closeErr: test.closeDir,
			}
			store := &FileStore{directory: "ignored", ops: fileStoreOps{
				openRead:  func(string) (durableFile, error) { return nil, os.ErrNotExist },
				openWrite: func(string) (durableFile, error) { return file, test.openFile },
				rename: func(string, string) error {
					events = append(events, "rename")
					return test.renameFile
				},
				openDir: func(string) (durableFile, error) { return directory, test.openDir },
			}}
			err := store.Save(samplePersistentState())
			if (err != nil) != test.wantError || !reflect.DeepEqual(events, test.want) {
				t.Fatalf("events=%v want=%v err=%v", events, test.want, err)
			}
		})
	}
}

func TestFileStoreIgnoresPartialUnpublishedTemporary(t *testing.T) {
	directory := t.TempDir()
	store, openErr := OpenFileStore(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	want := samplePersistentState()
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}
	newer := samplePersistentState()
	newer.HardState.Term = 8
	newer.HardState.VotedFor = 0
	encoded, err := encodeFileState(newer)
	if err != nil {
		t.Fatal(err)
	}
	if writeErr := os.WriteFile(filepath.Join(directory, fileStoreTemporary), encoded[:len(encoded)/2], 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	got, err := store.Load()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("Load state=%+v err=%v", got, err)
	}
}

func TestPersistentStateRejectsGapsAndTermRegression(t *testing.T) {
	for _, state := range []PersistentState{
		{HardState: HardState{Term: 2}, Entries: []Entry{{Index: 2, Term: 2, Type: EntryCommand}}},
		{HardState: HardState{Term: 2}, Entries: []Entry{{Index: 1, Term: 2, Type: EntryCommand}, {Index: 2, Term: 1, Type: EntryCommand}}},
		{HardState: HardState{Term: 0, VotedFor: 1}},
	} {
		if err := validatePersistent(state); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("state=%+v error=%v", state, err)
		}
	}
}

func TestFileStoreVoteAndLogSurviveNodeRestart(t *testing.T) {
	directory := t.TempDir()
	store, openErr := OpenFileStore(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	node, _ := newTestNode(t, 2, []NodeID{1, 2, 3}, store)
	responses, err := node.Step(Message{Type: RequestVote, From: 1, To: 2, Term: 4})
	if err != nil || !responses[0].VoteGranted {
		t.Fatalf("vote=%+v err=%v", responses, err)
	}
	responses, err = node.Step(Message{Type: AppendEntries, From: 1, To: 2, Term: 4, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []Entry{{Index: 1, Term: 4, Type: EntryCommand, Command: []byte("durable")}}})
	if err != nil || !responses[0].Success {
		t.Fatalf("append=%+v err=%v", responses, err)
	}
	reopened, err := OpenFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	restarted, _ := newTestNode(t, 2, []NodeID{1, 2, 3}, reopened)
	if status := restarted.Status(); status.Term != 4 || status.VotedFor != 1 || status.LastIndex != 1 || status.Role != Follower {
		t.Fatalf("restart status=%+v", status)
	}
	responses, err = restarted.Step(Message{Type: RequestVote, From: 3, To: 2, Term: 4, CandidateLastIndex: 1, CandidateLastTerm: 4})
	if err != nil || responses[0].VoteGranted {
		t.Fatalf("second vote=%+v err=%v", responses, err)
	}
}

func TestFileStoreDeterministicStress(t *testing.T) {
	if os.Getenv("RIVETDB_RAFT_STRESS") == "" {
		t.Skip("set RIVETDB_RAFT_STRESS=1 for durable Raft-store stress")
	}
	seed := testutil.Seed(t)
	rng := testutil.RandFromSeed(seed)
	directory := testutil.BenchmarkDir(t)
	store, openErr := OpenFileStore(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	state := PersistentState{}
	for operation := range 500 {
		if operation%17 == 0 {
			state.HardState.Term++
			state.HardState.VotedFor = NodeID(rng.Uint64N(5) + 1)
		}
		index := state.Snapshot.Index + uint64(len(state.Entries)) + 1 //nolint:gosec // bounded campaign
		command := []byte(fmt.Sprintf("%d:%d:%d", seed, operation, rng.Uint64()))
		state.Entries = append(state.Entries, Entry{Index: index, Term: state.HardState.Term, Type: EntryCommand, Command: command})
		if operation != 0 && operation%100 == 0 {
			boundary := state.Entries[49]
			state.Snapshot = Snapshot{Index: boundary.Index, Term: boundary.Term, Data: []byte(fmt.Sprintf("snapshot:%d", boundary.Index))}
			state.Entries = cloneEntries(state.Entries[50:])
		}
		if saveErr := store.Save(state); saveErr != nil {
			t.Fatalf("operation %d Save: %v", operation, saveErr)
		}
		if operation%11 == 0 {
			reopenedStore, reopenErr := OpenFileStore(directory)
			if reopenErr != nil {
				t.Fatal(reopenErr)
			}
			store = reopenedStore
			got, loadErr := store.Load()
			if loadErr != nil || !reflect.DeepEqual(got, state) {
				t.Fatalf("operation %d state mismatch err=%v", operation, loadErr)
			}
		}
	}
	t.Logf("seed=%d saves=500 snapshot_index=%d retained_entries=%d term=%d", seed, state.Snapshot.Index, len(state.Entries), state.HardState.Term)
}
