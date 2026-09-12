package advisor

// ValidateReplay validates recorded model output against the exact canonical
// input and digest without calling a model or changing audit/controller state.
func ValidateReplay(input SnapshotInput, raw []byte, cfg Config) (Advice, error) {
	snapshot, _, digest, err := BuildSnapshot(input, cfg)
	if err != nil {
		return Advice{}, err
	}
	return parseAdvice(raw, snapshot, digest, cfg)
}
