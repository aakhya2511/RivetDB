package compaction

import (
	"bytes"
	"slices"

	"github.com/rivetdb/rivetdb/internal/storage/manifest"
)

const DefaultL0Trigger = 4

// Picker deterministically selects oldest-seeded L0-to-L1 work.
type Picker struct{ L0Trigger int }

// Plan is an immutable description tied to a Version snapshot.
type Plan struct {
	BaseGeneration uint64
	SourceLevel    uint32
	TargetLevel    uint32
	SourceInputs   []manifest.TableMetadata
	TargetInputs   []manifest.TableMetadata
	SmallestUser   []byte
	LargestUser    []byte
	Reason         string
}

// Pick selects one compaction, or nil below the configured trigger.
func (p Picker) Pick(version *manifest.Version) (*Plan, error) {
	if version == nil {
		return nil, ErrInvalidPlan
	}
	trigger := p.L0Trigger
	if trigger == 0 {
		trigger = DefaultL0Trigger
	}
	if trigger < 1 {
		return nil, ErrInvalidOptions
	}
	source, target := version.Files(0), version.Files(1)
	if len(source) < trigger {
		return nil, nil
	}
	seed := source[len(source)-1]
	selectedSource, selectedTarget := map[uint64]bool{seed.FileNumber: true}, make(map[uint64]bool)
	smallest, largest := slices.Clone(seed.SmallestUser), slices.Clone(seed.LargestUser)
	for changed := true; changed; {
		changed = false
		for _, table := range source {
			if !selectedSource[table.FileNumber] && rangesOverlap(smallest, largest, table.SmallestUser, table.LargestUser) {
				selectedSource[table.FileNumber], changed = true, true
				expandRange(&smallest, &largest, table)
			}
		}
		for _, table := range target {
			if !selectedTarget[table.FileNumber] && rangesOverlap(smallest, largest, table.SmallestUser, table.LargestUser) {
				selectedTarget[table.FileNumber], changed = true, true
				expandRange(&smallest, &largest, table)
			}
		}
	}
	plan := &Plan{BaseGeneration: version.Generation(), SourceLevel: 0, TargetLevel: 1, SmallestUser: smallest, LargestUser: largest, Reason: "l0-file-count"}
	for _, table := range source {
		if selectedSource[table.FileNumber] {
			plan.SourceInputs = append(plan.SourceInputs, table)
		}
	}
	for _, table := range target {
		if selectedTarget[table.FileNumber] {
			plan.TargetInputs = append(plan.TargetInputs, table)
		}
	}
	if err := plan.Validate(version); err != nil {
		return nil, err
	}
	return plan, nil
}

// Validate proves plan inputs are live in the snapshot and its closure is stable.
func (p *Plan) Validate(version *manifest.Version) error {
	if p == nil || version == nil || p.SourceLevel != 0 || p.TargetLevel != 1 || len(p.SourceInputs) == 0 || bytes.Compare(p.SmallestUser, p.LargestUser) > 0 {
		return ErrInvalidPlan
	}
	selected := make(map[uint64]bool, len(p.SourceInputs)+len(p.TargetInputs))
	for _, table := range append(slices.Clone(p.SourceInputs), p.TargetInputs...) {
		if !version.Contains(table) || !rangesOverlap(p.SmallestUser, p.LargestUser, table.SmallestUser, table.LargestUser) {
			return ErrInvalidPlan
		}
		selected[table.FileNumber] = true
	}
	for _, level := range []uint32{p.SourceLevel, p.TargetLevel} {
		for _, table := range version.Files(level) {
			if rangesOverlap(p.SmallestUser, p.LargestUser, table.SmallestUser, table.LargestUser) && !selected[table.FileNumber] {
				return ErrInvalidPlan
			}
		}
	}
	return nil
}

func (p *Plan) Inputs() []manifest.TableMetadata {
	if p == nil {
		return nil
	}
	result := append([]manifest.TableMetadata(nil), p.SourceInputs...)
	return append(result, p.TargetInputs...)
}

func rangesOverlap(a0, a1, b0, b1 []byte) bool {
	return bytes.Compare(a0, b1) <= 0 && bytes.Compare(b0, a1) <= 0
}
func expandRange(smallest, largest *[]byte, table manifest.TableMetadata) {
	if bytes.Compare(table.SmallestUser, *smallest) < 0 {
		*smallest = slices.Clone(table.SmallestUser)
	}
	if bytes.Compare(table.LargestUser, *largest) > 0 {
		*largest = slices.Clone(table.LargestUser)
	}
}
