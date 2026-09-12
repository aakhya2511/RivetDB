package raft

import (
	"encoding/binary"
	"errors"
	"slices"
)

const membershipEncodingVersion = byte(1)

var ErrConfigTransition = errors.New("raft: invalid configuration transition")

func cloneConfiguration(value Configuration) Configuration {
	value.OldVoters = slices.Clone(value.OldVoters)
	value.NewVoters = slices.Clone(value.NewVoters)
	value.Learners = slices.Clone(value.Learners)
	return value
}

func normalizeConfiguration(value Configuration) Configuration {
	value = cloneConfiguration(value)
	slices.Sort(value.OldVoters)
	slices.Sort(value.NewVoters)
	slices.Sort(value.Learners)
	return value
}

func validateConfiguration(value Configuration) error {
	if value.Version == 0 || len(value.OldVoters) == 0 {
		return ErrInvalidConfig
	}
	value = normalizeConfiguration(value)
	seen := make(map[NodeID]struct{}, len(value.OldVoters)+len(value.NewVoters)+len(value.Learners))
	check := func(ids []NodeID, overlapVoters bool) bool {
		for index, id := range ids {
			if id == 0 || index > 0 && id == ids[index-1] {
				return false
			}
			if _, ok := seen[id]; ok && !overlapVoters {
				return false
			}
			seen[id] = struct{}{}
		}
		return true
	}
	if !check(value.OldVoters, false) {
		return ErrInvalidConfig
	}
	// Old and new voter sets intentionally overlap in joint consensus.
	for index, id := range value.NewVoters {
		if id == 0 || index > 0 && id == value.NewVoters[index-1] {
			return ErrInvalidConfig
		}
		seen[id] = struct{}{}
	}
	for index, id := range value.Learners {
		if id == 0 || index > 0 && id == value.Learners[index-1] || slices.Contains(value.OldVoters, id) || slices.Contains(value.NewVoters, id) {
			return ErrInvalidConfig
		}
	}
	return nil
}

func (c Configuration) Joint() bool { return len(c.NewVoters) != 0 }

func (c Configuration) Voter(id NodeID) bool {
	return slices.Contains(c.OldVoters, id) || slices.Contains(c.NewVoters, id)
}

func (c Configuration) Learner(id NodeID) bool { return slices.Contains(c.Learners, id) }

func (c Configuration) Members() []NodeID {
	result := append(slices.Clone(c.OldVoters), c.NewVoters...)
	result = append(result, c.Learners...)
	slices.Sort(result)
	return slices.Compact(result)
}

func majorityAcknowledged(voters []NodeID, acknowledged func(NodeID) bool) bool {
	count := 0
	for _, id := range voters {
		if acknowledged(id) {
			count++
		}
	}
	return count >= Quorum(len(voters))
}

func (c Configuration) Quorum(acknowledged func(NodeID) bool) bool {
	if !majorityAcknowledged(c.OldVoters, acknowledged) {
		return false
	}
	return !c.Joint() || majorityAcknowledged(c.NewVoters, acknowledged)
}

func EncodeConfiguration(value Configuration) ([]byte, error) {
	value = normalizeConfiguration(value)
	if err := validateConfiguration(value); err != nil {
		return nil, err
	}
	if len(value.OldVoters) > 255 || len(value.NewVoters) > 255 || len(value.Learners) > 255 {
		return nil, ErrResourceLimit
	}
	encoded := make([]byte, 12, 12+8*(len(value.OldVoters)+len(value.NewVoters)+len(value.Learners)))
	encoded[0] = membershipEncodingVersion
	encoded[1], encoded[2], encoded[3] = byte(len(value.OldVoters)), byte(len(value.NewVoters)), byte(len(value.Learners))
	binary.LittleEndian.PutUint64(encoded[4:12], value.Version)
	for _, ids := range [][]NodeID{value.OldVoters, value.NewVoters, value.Learners} {
		for _, id := range ids {
			encoded = binary.LittleEndian.AppendUint64(encoded, uint64(id))
		}
	}
	return encoded, nil
}

func DecodeConfiguration(encoded []byte) (Configuration, error) {
	if len(encoded) < 12 || encoded[0] != membershipEncodingVersion {
		return Configuration{}, ErrInvalidConfig
	}
	counts := []int{int(encoded[1]), int(encoded[2]), int(encoded[3])}
	if len(encoded) != 12+8*(counts[0]+counts[1]+counts[2]) {
		return Configuration{}, ErrInvalidConfig
	}
	result := Configuration{Version: binary.LittleEndian.Uint64(encoded[4:12])}
	offset := 12
	sets := []*[]NodeID{&result.OldVoters, &result.NewVoters, &result.Learners}
	for setIndex, count := range counts {
		for range count {
			*sets[setIndex] = append(*sets[setIndex], NodeID(binary.LittleEndian.Uint64(encoded[offset:offset+8])))
			offset += 8
		}
	}
	if err := validateConfiguration(result); err != nil {
		return Configuration{}, err
	}
	return normalizeConfiguration(result), nil
}

func validateEncodedConfiguration(encoded []byte) error {
	_, err := DecodeConfiguration(encoded)
	return err
}

func validateConfigurationTransition(from, to Configuration) error {
	from, to = normalizeConfiguration(from), normalizeConfiguration(to)
	if err := validateConfiguration(from); err != nil {
		return err
	}
	if err := validateConfiguration(to); err != nil {
		return err
	}
	if to.Version != from.Version+1 {
		return ErrConfigTransition
	}
	if from.Joint() {
		if to.Joint() || !slices.Equal(to.OldVoters, from.NewVoters) {
			return ErrConfigTransition
		}
		return nil
	}
	if to.Joint() {
		if !slices.Equal(to.OldVoters, from.OldVoters) {
			return ErrConfigTransition
		}
		return nil
	}
	// Stable-to-stable is learner-only: voters cannot change.
	if !slices.Equal(to.OldVoters, from.OldVoters) {
		return ErrConfigTransition
	}
	return nil
}
