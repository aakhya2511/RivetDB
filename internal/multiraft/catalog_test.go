package multiraft

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func threeRangeBootstrap() Bootstrap {
	return Bootstrap{Generation: 7, Nodes: []raft.NodeID{1, 2, 3, 4, 5}, ReplicationFactor: 3, Ranges: []RangeDescriptor{
		{RangeID: 10, Generation: 1, StartKey: KeyBound{Unbounded: true}, EndKey: KeyBound{Key: []byte("g")}, Replicas: replicas(1, 2, 3)},
		{RangeID: 11, Generation: 2, StartKey: KeyBound{Key: []byte("g")}, EndKey: KeyBound{Key: []byte("p")}, Replicas: replicas(2, 3, 4)},
		{RangeID: 12, Generation: 3, StartKey: KeyBound{Key: []byte("p")}, EndKey: KeyBound{Unbounded: true}, Replicas: replicas(1, 3, 5)},
	}}
}

func replicas(nodes ...raft.NodeID) []ReplicaDescriptor {
	result := make([]ReplicaDescriptor, len(nodes))
	for index, nodeID := range nodes {
		result[index] = ReplicaDescriptor{ReplicaID: ReplicaID(100 + index + int(nodeID)*10), NodeID: nodeID}
	}
	return result
}

func TestCatalogBoundaryLookupAndOwnedSnapshots(t *testing.T) {
	catalog, err := NewCatalog(threeRangeBootstrap())
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		key  []byte
		want RangeID
	}{
		{nil, 10}, {[]byte{0}, 10}, {[]byte("a"), 10}, {[]byte("f\xff"), 10},
		{[]byte("g"), 11}, {[]byte("o\xff"), 11}, {[]byte("p"), 12}, {[]byte{0xff}, 12},
	}
	for _, testCase := range cases {
		descriptor, lookupErr := catalog.Lookup(testCase.key)
		if lookupErr != nil || descriptor.RangeID != testCase.want {
			t.Fatalf("key=%x range=%d err=%v", testCase.key, descriptor.RangeID, lookupErr)
		}
	}
	descriptor, _ := catalog.Lookup([]byte("g"))
	descriptor.StartKey.Key[0] = 'x'
	descriptor.Replicas[0].NodeID = 99
	again, _ := catalog.LookupByID(11)
	if string(again.StartKey.Key) != "g" || again.Replicas[0].NodeID == 99 {
		t.Fatal("catalog leaked mutable descriptor storage")
	}
	permuted := cloneBootstrap(threeRangeBootstrap())
	for index := range permuted.Ranges {
		replicas := permuted.Ranges[index].Replicas
		replicas[0], replicas[len(replicas)-1] = replicas[len(replicas)-1], replicas[0]
	}
	canonical, err := NewCatalog(permuted)
	if err != nil || canonical.Fingerprint() != catalog.Fingerprint() {
		t.Fatalf("replica order changed canonical catalog: %v", err)
	}
}

func TestCatalogRejectsInvalidLayoutsAndDescriptors(t *testing.T) {
	base := threeRangeBootstrap()
	tests := []struct {
		name   string
		mutate func(*Bootstrap)
	}{
		{"gap", func(value *Bootstrap) { value.Ranges[1].StartKey.Key = []byte("h") }},
		{"overlap", func(value *Bootstrap) { value.Ranges[1].StartKey.Key = []byte("f") }},
		{"missing-negative-infinity", func(value *Bootstrap) { value.Ranges[0].StartKey = KeyBound{Key: []byte{}} }},
		{"missing-positive-infinity", func(value *Bootstrap) { value.Ranges[2].EndKey = KeyBound{Key: []byte("z")} }},
		{"duplicate-range", func(value *Bootstrap) { value.Ranges[1].RangeID = 10 }},
		{"zero-generation", func(value *Bootstrap) { value.Ranges[1].Generation = 0 }},
		{"empty-interval", func(value *Bootstrap) { value.Ranges[1].EndKey.Key = []byte("g") }},
		{"duplicate-replica", func(value *Bootstrap) { value.Ranges[1].Replicas[1].ReplicaID = value.Ranges[1].Replicas[0].ReplicaID }},
		{"duplicate-node", func(value *Bootstrap) { value.Ranges[1].Replicas[1].NodeID = value.Ranges[1].Replicas[0].NodeID }},
		{"unknown-node", func(value *Bootstrap) { value.Ranges[1].Replicas[1].NodeID = 99 }},
		{"wrong-rf", func(value *Bootstrap) { value.Ranges[1].Replicas = value.Ranges[1].Replicas[:2] }},
		{"unbounded-has-bytes", func(value *Bootstrap) { value.Ranges[0].StartKey.Key = []byte("bad") }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			candidate := cloneBootstrap(base)
			testCase.mutate(&candidate)
			if _, err := NewCatalog(candidate); err == nil {
				t.Fatal("invalid catalog accepted")
			}
		})
	}
}

func TestPartialCatalogReturnsRangeNotFound(t *testing.T) {
	bootstrap := threeRangeBootstrap()
	bootstrap.AllowPartial = true
	bootstrap.Ranges = bootstrap.Ranges[1:2]
	catalog, err := NewCatalog(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Lookup([]byte("a")); !errors.Is(err, ErrRangeNotFound) {
		t.Fatalf("partial miss=%v", err)
	}
}

func TestCatalogCodecRoundTripAndHostileInput(t *testing.T) {
	catalog, err := NewCatalog(threeRangeBootstrap())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCatalog(encoded)
	if err != nil || !catalogsEqual(catalog, decoded) {
		t.Fatalf("round trip equal=%v err=%v", catalogsEqual(catalog, decoded), err)
	}
	for offset := range encoded {
		corrupt := bytes.Clone(encoded)
		corrupt[offset] ^= 0x80
		if _, err := decodeCatalog(corrupt); err == nil {
			t.Fatalf("corruption at %d accepted", offset)
		}
	}
	for length := range encoded {
		if _, err := decodeCatalog(encoded[:length]); err == nil {
			t.Fatalf("truncation at %d accepted", length)
		}
	}
}

func TestCatalogPersistenceAuthorityAndPublicationStages(t *testing.T) {
	root := t.TempDir()
	bootstrap := threeRangeBootstrap()
	var stages []CatalogPublishStage
	catalog, err := LoadOrBootstrapCatalog(root, &bootstrap, func(stage CatalogPublishStage) error {
		stages = append(stages, stage)
		return nil
	})
	if err != nil || len(stages) != 3 {
		t.Fatalf("bootstrap stages=%v err=%v", stages, err)
	}
	recovered, err := LoadOrBootstrapCatalog(root, nil, nil)
	if err != nil || !catalogsEqual(catalog, recovered) {
		t.Fatalf("recover equal=%v err=%v", catalogsEqual(catalog, recovered), err)
	}
	mismatch := cloneBootstrap(bootstrap)
	mismatch.Ranges[0].Generation++
	if _, err := LoadOrBootstrapCatalog(root, &mismatch, nil); !errors.Is(err, ErrCatalogMismatch) {
		t.Fatalf("mismatch=%v", err)
	}
	orphan := filepath.Join(root, "ranges", "99")
	if err := os.MkdirAll(orphan, 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrBootstrapCatalog(root, nil, nil); err != nil {
		t.Fatalf("directory incorrectly became authority: %v", err)
	}
}

func TestCatalogReplacementRequiresGenerationAndSupportsFutureSplitShape(t *testing.T) {
	root := t.TempDir()
	bootstrap := threeRangeBootstrap()
	current, err := LoadOrBootstrapCatalog(root, &bootstrap, nil)
	if err != nil {
		t.Fatal(err)
	}
	stale := cloneBootstrap(bootstrap)
	stale.Generation++
	stale.Ranges[0].EndKey.Key = []byte("m")
	stale.Ranges[1].StartKey.Key = []byte("m")
	if _, replaceErr := ReplaceCatalog(root, current.Fingerprint(), stale, nil); replaceErr == nil {
		t.Fatal("changed descriptor without generation increase")
	}
	next := cloneBootstrap(bootstrap)
	next.Generation++
	next.Ranges[0].EndKey.Key = []byte("d")
	next.Ranges[0].Generation++
	next.Ranges = append(next.Ranges, RangeDescriptor{RangeID: 20, Generation: 1, StartKey: KeyBound{Key: []byte("d")}, EndKey: KeyBound{Key: []byte("g")}, Replicas: replicas(1, 2, 3)})
	replacement, err := ReplaceCatalog(root, current.Fingerprint(), next, nil)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor, lookupErr := replacement.Lookup([]byte("d")); lookupErr != nil || descriptor.RangeID != 20 {
		t.Fatalf("split-shaped lookup=%d err=%v", descriptor.RangeID, lookupErr)
	}
	if _, err := ReplaceCatalog(root, current.Fingerprint(), next, nil); !errors.Is(err, ErrCatalogMismatch) {
		t.Fatalf("stale compare-and-swap=%v", err)
	}
}

func TestNodesRejectMismatchedStaticDescriptors(t *testing.T) {
	leftBootstrap := threeRangeBootstrap()
	rightBootstrap := cloneBootstrap(leftBootstrap)
	rightBootstrap.Generation++
	rightBootstrap.Ranges[0].EndKey.Key = []byte("h")
	rightBootstrap.Ranges[1].StartKey.Key = []byte("h")
	left, err := OpenNode(NodeOptions{NodeID: 1, Directory: filepath.Join(t.TempDir(), "left"), Bootstrap: &leftBootstrap})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := left.Close(context.Background()); closeErr != nil {
			t.Fatal(closeErr)
		}
	}()
	right, err := OpenNode(NodeOptions{NodeID: 2, Directory: filepath.Join(t.TempDir(), "right"), Bootstrap: &rightBootstrap})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := right.Close(context.Background()); closeErr != nil {
			t.Fatal(closeErr)
		}
	}()
	if validateErr := ValidateNodeCatalogs(left, right); !errors.Is(validateErr, ErrCatalogMismatch) {
		t.Fatalf("mismatched descriptors=%v", validateErr)
	}
	var outbound []Envelope
	for range 20 {
		outbound, err = left.Tick(10)
		if err != nil {
			t.Fatal(err)
		}
		if len(outbound) != 0 {
			break
		}
	}
	if len(outbound) == 0 {
		t.Fatal("mismatched node emitted no Raft message")
	}
	for _, envelope := range outbound {
		if envelope.Message.To == 2 {
			if _, stepErr := right.Step(envelope); !errors.Is(stepErr, ErrCatalogMismatch) {
				t.Fatalf("descriptor fingerprint mismatch=%v", stepErr)
			}
			return
		}
	}
	t.Fatal("mismatched node emitted no message to node 2")
}

func TestCatalogLookupMatchesReferenceRandomized(t *testing.T) {
	for _, seed := range testutil.SeedCorpus(t, t.Name()) {
		randomCatalogLookup(t, seed)
	}
	randomCatalogLookup(t, testutil.Seed(t))
}

func randomCatalogLookup(t *testing.T, seed int64) {
	t.Helper()
	rng := testutil.RandFromSeed(seed)
	const rangeCount = 128
	ranges := make([]RangeDescriptor, rangeCount)
	for index := range ranges {
		start := KeyBound{Key: []byte{byte(index)}}
		end := KeyBound{Key: []byte{byte(index + 1)}}
		if index == 0 {
			start = KeyBound{Unbounded: true}
		}
		if index == rangeCount-1 {
			end = KeyBound{Unbounded: true}
		}
		ranges[index] = RangeDescriptor{RangeID: RangeID(index + 1), Generation: 1, StartKey: start, EndKey: end,
			Replicas: []ReplicaDescriptor{{ReplicaID: ReplicaID(index + 1), NodeID: 1}}}
	}
	catalog, err := NewCatalog(Bootstrap{Generation: 1, Nodes: []raft.NodeID{1}, ReplicationFactor: 1, Ranges: ranges})
	if err != nil {
		t.Fatal(err)
	}
	for iteration := 0; iteration < 10_000; iteration++ {
		key := randomBinaryKey(rng)
		got, lookupErr := catalog.Lookup(key)
		if lookupErr != nil {
			t.Fatalf("seed=%d key=%x: %v", seed, key, lookupErr)
		}
		var want RangeID
		for _, descriptor := range ranges {
			if descriptor.Contains(key) {
				want = descriptor.RangeID
				break
			}
		}
		if got.RangeID != want {
			t.Fatalf("seed=%d key=%x got=%d want=%d", seed, key, got.RangeID, want)
		}
	}
}

func randomBinaryKey(rng *rand.Rand) []byte {
	result := make([]byte, rng.IntN(5))
	for index := range result {
		result[index] = byte(rng.Uint32())
	}
	return result
}

func cloneBootstrap(value Bootstrap) Bootstrap {
	result := value
	result.Nodes = append([]raft.NodeID(nil), value.Nodes...)
	result.Ranges = make([]RangeDescriptor, len(value.Ranges))
	for index := range value.Ranges {
		result.Ranges[index] = cloneDescriptor(value.Ranges[index])
	}
	return result
}
