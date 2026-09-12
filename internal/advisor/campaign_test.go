package advisor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/rivetdb/rivetdb/internal/multiraft"
)

func TestRandomizedAdvisoryCampaign10000(t *testing.T) {
	input, policy, cooldowns := testInput()
	service, model, _ := newService(t, nil)
	valid := validAdvice(input, ActionTransferLeader, 20)
	unknown := validAdvice(input, ActionTransferLeader, 999)
	noop := validAdvice(input, ActionNoop, 0)
	var stale Advice
	_ = json.Unmarshal(valid, &stale)
	stale.CatalogGeneration++
	staleRaw, _ := json.Marshal(stale)
	var conflict Advice
	_ = json.Unmarshal(valid, &conflict)
	other := conflict.Actions[0]
	other.Type = ActionSplit
	conflict.Actions = append(conflict.Actions, other)
	conflictRaw, _ := json.Marshal(conflict)
	var ungrounded Advice
	_ = json.Unmarshal(valid, &ungrounded)
	ungrounded.Findings[0].Evidence[0].Value++
	ungroundedRaw, _ := json.Marshal(ungrounded)
	oversized := make([]byte, service.cfg.MaxOutputBytes+1)
	for cycle := 0; cycle < 10_000; cycle++ {
		var raw []byte
		switch cycle % 8 {
		case 0:
			raw = valid
		case 1:
			raw = unknown
		case 2:
			raw = staleRaw
		case 3:
			raw = []byte("{")
		case 4:
			raw = conflictRaw
		case 5:
			raw = noop
		case 6:
			raw = ungroundedRaw
		default:
			raw = oversized
		}
		model.mu.Lock()
		model.Response = raw
		model.mu.Unlock()
		advice, err := service.Analyze(context.Background(), input)
		if cycle%8 >= 2 && cycle%8 != 5 {
			if err == nil {
				t.Fatalf("cycle %d accepted invalid advice", cycle)
			}
			continue
		}
		if err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		if cycle%8 == 5 {
			continue
		}
		_, err = service.Approve(Approval{AdviceID: advice.AdviceID, ActionIndex: 0, Operator: "campaign"}, input, input.Cluster, policy, cooldowns)
		if cycle%8 == 0 && err != nil {
			t.Fatalf("valid cycle %d: %v", cycle, err)
		}
		if cycle%8 == 1 && !errors.Is(err, ErrUnknownIdentity) {
			t.Fatalf("unknown cycle %d: %v", cycle, err)
		}
	}
	c := service.Counters()
	if c.Generated != 3750 || c.Valid != 3750 || c.Invalid != 6250 || c.Stale != 1250 || c.UnknownIDs != 1250 || c.Conflicts != 1250 || c.ValidatorAccepted != 1250 || c.ValidatorRejected != 1250 || c.SafetyViolations != 0 {
		t.Fatalf("campaign counters=%+v", c)
	}
	t.Logf("cycles=10000 generated=%d invalid=%d stale=%d unknown=%d conflicts=%d accepted=%d rejected=%d safety=%d", c.Generated, c.Invalid, c.Stale, c.UnknownIDs, c.Conflicts, c.ValidatorAccepted, c.ValidatorRejected, c.SafetyViolations)
}

func TestShadowAgreementAndDisagreementAreObservational(t *testing.T) {
	input, policy, cooldowns := testInput()
	service, model, _ := newService(t, validAdvice(input, ActionTransferLeader, 20))
	advice, err := service.Analyze(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := multiraft.PlanRebalance(input.Cluster, policy, cooldowns)
	if err != nil {
		t.Fatal(err)
	}
	service.ObservePlanner(advice, plan)
	model.mu.Lock()
	model.Response = validAdvice(input, ActionSplit, 20)
	model.mu.Unlock()
	different, err := service.Analyze(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	service.ObservePlanner(different, plan)
	c := service.Counters()
	if c.PlannerAgreement != 1 || c.DifferentValidAction != 1 {
		t.Fatalf("agreement counters=%+v", c)
	}
}
