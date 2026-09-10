package compaction

import "fmt"

type State uint8

const (
	StatePlanned State = iota
	StateRunning
	StateOutputDurable
	StateInstalling
	StateInstalled
	StateFailed
	StateStale
)

func (s State) String() string {
	names := [...]string{"planned", "running", "output-durable", "installing", "installed", "failed", "stale"}
	if int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("unknown(%d)", s)
}
func validTransition(from, to State) bool {
	return from == StatePlanned && to == StateRunning || from == StateRunning && (to == StateOutputDurable || to == StateFailed) || from == StateOutputDurable && (to == StateInstalling || to == StateStale || to == StateFailed) || from == StateInstalling && (to == StateInstalled || to == StateFailed || to == StateStale)
}
