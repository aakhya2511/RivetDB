package multiraft

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"slices"
	"sort"

	"github.com/rivetdb/rivetdb/internal/raft"
)

const (
	MaxCatalogRanges = 4096
	MaxCatalogNodes  = 1024
	MaxRangeReplicas = 31
)

type Bootstrap struct {
	Generation        uint64
	Nodes             []raft.NodeID
	ReplicationFactor int
	AllowPartial      bool
	Ranges            []RangeDescriptor
}

// Catalog is immutable after construction.
type Catalog struct {
	generation        uint64
	nodes             []raft.NodeID
	replicationFactor int
	allowPartial      bool
	ranges            []RangeDescriptor
	byID              map[RangeID]int
	fingerprint       [sha256.Size]byte
}

func NewCatalog(bootstrap Bootstrap) (*Catalog, error) {
	if bootstrap.Generation == 0 || len(bootstrap.Nodes) == 0 || len(bootstrap.Nodes) > MaxCatalogNodes ||
		bootstrap.ReplicationFactor < 1 || bootstrap.ReplicationFactor > MaxRangeReplicas ||
		len(bootstrap.Ranges) == 0 || len(bootstrap.Ranges) > MaxCatalogRanges {
		return nil, ErrInvalidCatalog
	}
	nodes := slices.Clone(bootstrap.Nodes)
	slices.Sort(nodes)
	for index, nodeID := range nodes {
		if nodeID == 0 || index > 0 && nodeID == nodes[index-1] {
			return nil, fmt.Errorf("%w: invalid or duplicate node", ErrInvalidCatalog)
		}
	}
	known := make(map[raft.NodeID]struct{}, len(nodes))
	for _, nodeID := range nodes {
		known[nodeID] = struct{}{}
	}
	ranges := make([]RangeDescriptor, len(bootstrap.Ranges))
	for index, descriptor := range bootstrap.Ranges {
		if err := descriptor.Validate(); err != nil {
			return nil, fmt.Errorf("validate range %d: %w", descriptor.RangeID, err)
		}
		if len(descriptor.Replicas) != bootstrap.ReplicationFactor {
			return nil, fmt.Errorf("%w: range %d replication factor", ErrInvalidCatalog, descriptor.RangeID)
		}
		for _, replica := range descriptor.Replicas {
			if _, exists := known[replica.NodeID]; !exists {
				return nil, fmt.Errorf("%w: range %d references unknown node %d", ErrInvalidCatalog, descriptor.RangeID, replica.NodeID)
			}
		}
		ranges[index] = cloneDescriptor(descriptor)
		sort.Slice(ranges[index].Replicas, func(left, right int) bool {
			if ranges[index].Replicas[left].ReplicaID != ranges[index].Replicas[right].ReplicaID {
				return ranges[index].Replicas[left].ReplicaID < ranges[index].Replicas[right].ReplicaID
			}
			return ranges[index].Replicas[left].NodeID < ranges[index].Replicas[right].NodeID
		})
	}
	sort.Slice(ranges, func(left, right int) bool { return compareStart(ranges[left], ranges[right]) < 0 })
	byID := make(map[RangeID]int, len(ranges))
	for index := range ranges {
		descriptor := ranges[index]
		if _, exists := byID[descriptor.RangeID]; exists {
			return nil, fmt.Errorf("%w: duplicate range %d", ErrInvalidCatalog, descriptor.RangeID)
		}
		byID[descriptor.RangeID] = index
		if index > 0 {
			previous := ranges[index-1]
			if previous.EndKey.Unbounded || descriptor.StartKey.Unbounded {
				return nil, fmt.Errorf("%w: internal unbounded endpoint", ErrInvalidCatalog)
			}
			comparison := bytes.Compare(previous.EndKey.Key, descriptor.StartKey.Key)
			if comparison > 0 || !bootstrap.AllowPartial && comparison != 0 {
				return nil, fmt.Errorf("%w: overlap or gap before range %d", ErrInvalidCatalog, descriptor.RangeID)
			}
		}
	}
	if !bootstrap.AllowPartial && (!ranges[0].StartKey.Unbounded || !ranges[len(ranges)-1].EndKey.Unbounded) {
		return nil, fmt.Errorf("%w: full keyspace is not covered", ErrInvalidCatalog)
	}
	catalog := &Catalog{generation: bootstrap.Generation, nodes: nodes, replicationFactor: bootstrap.ReplicationFactor,
		allowPartial: bootstrap.AllowPartial, ranges: ranges, byID: byID}
	encoded, err := encodeCatalog(catalog)
	if err != nil {
		return nil, err
	}
	catalog.fingerprint = sha256.Sum256(encoded)
	return catalog, nil
}

func compareStart(left, right RangeDescriptor) int {
	if left.StartKey.Unbounded {
		if right.StartKey.Unbounded {
			return 0
		}
		return -1
	}
	if right.StartKey.Unbounded {
		return 1
	}
	return bytes.Compare(left.StartKey.Key, right.StartKey.Key)
}

func (c *Catalog) Lookup(key []byte) (RangeDescriptor, error) {
	position := sort.Search(len(c.ranges), func(index int) bool {
		return c.ranges[index].EndKey.Unbounded || bytes.Compare(key, c.ranges[index].EndKey.Key) < 0
	})
	if position == len(c.ranges) || !c.ranges[position].Contains(key) {
		return RangeDescriptor{}, ErrRangeNotFound
	}
	return cloneDescriptor(c.ranges[position]), nil
}

func (c *Catalog) LookupByID(rangeID RangeID) (RangeDescriptor, error) {
	position, exists := c.byID[rangeID]
	if !exists {
		return RangeDescriptor{}, ErrRangeNotFound
	}
	return cloneDescriptor(c.ranges[position]), nil
}

func (c *Catalog) Snapshot() Bootstrap {
	ranges := make([]RangeDescriptor, len(c.ranges))
	for index := range c.ranges {
		ranges[index] = cloneDescriptor(c.ranges[index])
	}
	return Bootstrap{Generation: c.generation, Nodes: slices.Clone(c.nodes), ReplicationFactor: c.replicationFactor,
		AllowPartial: c.allowPartial, Ranges: ranges}
}

func (c *Catalog) Fingerprint() [sha256.Size]byte { return c.fingerprint }
func (c *Catalog) Generation() uint64             { return c.generation }

func (c *Catalog) Assigned(nodeID raft.NodeID) []RangeDescriptor {
	var result []RangeDescriptor
	for _, descriptor := range c.ranges {
		if _, exists := descriptor.ReplicaOn(nodeID); exists {
			result = append(result, cloneDescriptor(descriptor))
		}
	}
	return result
}

func catalogsEqual(left, right *Catalog) bool {
	return left.fingerprint == right.fingerprint
}
