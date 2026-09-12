package advisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rivetdb/rivetdb/internal/multiraft"
)

const systemPolicy = "You are an advisory RivetDB operator. Use only supplied data. Emit only the schema. Never invent IDs or authority. Mark uncertainty."

type modelRequest struct {
	PromptVersion string          `json:"prompt_version"`
	Policy        string          `json:"policy"`
	Snapshot      json.RawMessage `json:"snapshot"`
}
type modelResult struct {
	output []byte
	err    error
}

type Service struct {
	mu          sync.Mutex
	cfg         Config
	model       AdvisorModel
	sem         chan struct{}
	nextID      uint64
	lastCall    time.Time
	hasLastCall bool
	records     map[uint64]AdviceRecord
	order       []uint64
	counters    Counters
}

func New(cfg Config, model AdvisorModel) (*Service, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Enabled && model == nil {
		return nil, ErrInvalidConfig
	}
	return &Service{cfg: cfg, model: model, sem: make(chan struct{}, cfg.MaxConcurrentAdviceRequests), nextID: 1, records: make(map[uint64]AdviceRecord)}, nil
}

func (s *Service) Analyze(ctx context.Context, input SnapshotInput) (Advice, error) {
	if !s.cfg.Enabled {
		return Advice{}, ErrDisabled
	}
	select {
	case s.sem <- struct{}{}:
	default:
		return Advice{}, ErrBusy
	}
	releaseByCaller := true
	defer func() {
		if releaseByCaller {
			<-s.sem
		}
	}()
	if s.cfg.MinAdviceInterval > 0 {
		s.mu.Lock()
		now := s.cfg.Clock.Now()
		if s.hasLastCall && now.Before(s.lastCall.Add(s.cfg.MinAdviceInterval)) {
			s.mu.Unlock()
			return Advice{}, ErrRateLimited
		}
		s.lastCall, s.hasLastCall = now, true
		s.mu.Unlock()
	}
	snapshot, snapshotBytes, digest, err := BuildSnapshot(input, s.cfg)
	if err != nil {
		s.increment(func(c *Counters) { c.Invalid++ })
		return Advice{}, err
	}
	request, err := json.Marshal(modelRequest{PromptVersion: PromptVersion, Policy: systemPolicy, Snapshot: snapshotBytes})
	if err != nil {
		return Advice{}, fmt.Errorf("marshal advisor request: %w", err)
	}
	if len(request) > int(s.cfg.MaxInputBytes) {
		return Advice{}, ErrInputTooLarge
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	resultCh := make(chan modelResult, 1)
	releaseByCaller = false
	go func() {
		var result modelResult
		defer func() {
			if recovered := recover(); recovered != nil {
				result.err = fmt.Errorf("%w: panic", ErrModelFailure)
			}
			<-s.sem
			resultCh <- result
		}()
		result.output, result.err = s.model.Analyze(callCtx, request)
	}()
	var result modelResult
	select {
	case <-callCtx.Done():
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			s.increment(func(c *Counters) { c.Timeouts++ })
			return Advice{}, ErrAdvisorTimeout
		}
		s.increment(func(c *Counters) { c.ModelFailures++ })
		return Advice{}, fmt.Errorf("%w: %w", ErrModelFailure, callCtx.Err())
	case result = <-resultCh:
	}
	if result.err != nil {
		s.increment(func(c *Counters) { c.ModelFailures++ })
		if errors.Is(result.err, context.DeadlineExceeded) || errors.Is(result.err, context.Canceled) && errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			s.increment(func(c *Counters) { c.Timeouts++ })
			return Advice{}, ErrAdvisorTimeout
		}
		return Advice{}, fmt.Errorf("%w: %w", ErrModelFailure, result.err)
	}
	advice, err := parseAdvice(result.output, snapshot, digest, s.cfg)
	if err != nil {
		s.incrementParseError(err)
		return Advice{}, err
	}
	s.mu.Lock()
	advice.AdviceID = s.nextID
	s.nextID++
	advice.Status = StatusGenerated
	record := AdviceRecord{AdviceID: advice.AdviceID, At: s.cfg.Clock.Now(), SnapshotDigest: digest, CatalogGeneration: advice.CatalogGeneration, PolicyVersion: advice.PolicyVersion, Provider: s.cfg.Provider, Model: s.cfg.Model, PromptVersion: PromptVersion, SchemaVersion: SchemaVersion, Advice: cloneAdvice(advice)}
	s.records[advice.AdviceID] = record
	s.order = append(s.order, advice.AdviceID)
	s.trimLocked()
	s.counters.Generated++
	s.counters.Valid++
	for _, a := range advice.Actions {
		if a.Type == ActionNoop || a.Type == ActionWait {
			s.counters.AdvisorNoop++
		}
	}
	s.mu.Unlock()
	return advice, nil
}

func (s *Service) increment(fn func(*Counters)) { s.mu.Lock(); fn(&s.counters); s.mu.Unlock() }
func (s *Service) incrementParseError(err error) {
	s.increment(func(c *Counters) {
		c.Invalid++
		if errors.Is(err, ErrStaleAdvice) {
			c.Stale++
		}
		if errors.Is(err, ErrUnknownIdentity) {
			c.UnknownIDs++
		}
		if errors.Is(err, ErrConflictingAdvice) {
			c.Conflicts++
		}
	})
}
func (s *Service) trimLocked() {
	for len(s.order) > int(s.cfg.MaxRecentActions) {
		id := s.order[0]
		s.order = s.order[1:]
		delete(s.records, id)
	}
}
func (s *Service) Counters() Counters { s.mu.Lock(); defer s.mu.Unlock(); return s.counters }
func (s *Service) Record(id uint64) (AdviceRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[id]
	if !ok {
		return AdviceRecord{}, false
	}
	r.Advice = cloneAdvice(r.Advice)
	r.ValidatorOutcomes = cloneValidationOutcomes(r.ValidatorOutcomes)
	return r, true
}

func cloneAdvice(advice Advice) Advice {
	advice.Findings = append([]Finding(nil), advice.Findings...)
	for index := range advice.Findings {
		advice.Findings[index].Evidence = append([]Evidence(nil), advice.Findings[index].Evidence...)
	}
	advice.Actions = append([]ProposedAction(nil), advice.Actions...)
	for index := range advice.Actions {
		advice.Actions[index].Constraints = append([]string(nil), advice.Actions[index].Constraints...)
		advice.Actions[index].Evidence = append([]Evidence(nil), advice.Actions[index].Evidence...)
	}
	advice.Limitations = append([]string(nil), advice.Limitations...)
	return advice
}

func cloneValidationOutcomes(outcomes []ValidationOutcome) []ValidationOutcome {
	cloned := append([]ValidationOutcome(nil), outcomes...)
	for index := range cloned {
		cloned[index].Action.SplitKey = append([]byte(nil), cloned[index].Action.SplitKey...)
		cloned[index].Action.Constraints = append([]string(nil), cloned[index].Action.Constraints...)
	}
	return cloned
}

// ObservePlanner records shadow-mode agreement without changing either plan.
func (s *Service) ObservePlanner(advice Advice, plan multiraft.RebalancePlan) {
	var proposed *ProposedAction
	for index := range advice.Actions {
		if _, executable := rebalanceType(advice.Actions[index].Type); executable {
			proposed = &advice.Actions[index]
			break
		}
	}
	s.increment(func(c *Counters) {
		if proposed == nil {
			c.AdvisorNoop++
			return
		}
		wanted, _ := rebalanceType(proposed.Type)
		for _, action := range plan.Actions {
			if action.Type == wanted && uint64(action.RangeID) == proposed.RangeID {
				c.PlannerAgreement++
				return
			}
		}
		c.DifferentValidAction++
	})
}
