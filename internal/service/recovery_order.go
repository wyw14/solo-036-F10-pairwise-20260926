package service

import (
	"errors"
	"fmt"
	"sort"
)

type RecoveryStage string

const (
	RecoveryJournal  RecoveryStage = "journal"
	RecoveryRuns     RecoveryStage = "runs"
	RecoveryRoutes   RecoveryStage = "routes"
	RecoveryPermits  RecoveryStage = "permits"
	RecoveryPressure RecoveryStage = "pressure"
)

type RecoveryStep struct {
	Stage     RecoveryStage   `json:"stage"`
	Requires  []RecoveryStage `json:"requires"`
	Completed bool            `json:"completed"`
}

var canonicalRecoveryOrder = []RecoveryStage{
	RecoveryJournal,
	RecoveryRuns,
	RecoveryRoutes,
	RecoveryPermits,
	RecoveryPressure,
}

type RecoveryPlan struct {
	steps map[RecoveryStage]RecoveryStep
}

func NewRecoveryPlan() *RecoveryPlan {
	return &RecoveryPlan{steps: map[RecoveryStage]RecoveryStep{
		RecoveryJournal:  {Stage: RecoveryJournal, Requires: []RecoveryStage{}},
		RecoveryRuns:     {Stage: RecoveryRuns, Requires: []RecoveryStage{RecoveryJournal}},
		RecoveryRoutes:   {Stage: RecoveryRoutes, Requires: []RecoveryStage{RecoveryJournal, RecoveryRuns}},
		RecoveryPermits:  {Stage: RecoveryPermits, Requires: []RecoveryStage{RecoveryJournal, RecoveryRuns, RecoveryRoutes}},
		RecoveryPressure: {Stage: RecoveryPressure, Requires: []RecoveryStage{RecoveryJournal, RecoveryRuns, RecoveryRoutes, RecoveryPermits}},
	}}
}

func (p *RecoveryPlan) Complete(stage RecoveryStage) error {
	step, ok := p.steps[stage]
	if !ok {
		return fmt.Errorf("unknown recovery stage %s", stage)
	}
	for _, dependency := range step.Requires {
		if !p.steps[dependency].Completed {
			return fmt.Errorf("recovery stage %s requires %s", stage, dependency)
		}
	}
	step.Completed = true
	p.steps[stage] = step
	return nil
}

func (p *RecoveryPlan) Ready(stage RecoveryStage) bool {
	step, ok := p.steps[stage]
	if !ok {
		return false
	}
	for _, dependency := range step.Requires {
		if !p.steps[dependency].Completed {
			return false
		}
	}
	return true
}

func (p *RecoveryPlan) Completed(stage RecoveryStage) bool {
	step, ok := p.steps[stage]
	return ok && step.Completed
}

func (p *RecoveryPlan) Steps() []RecoveryStep {
	order := map[RecoveryStage]int{
		RecoveryJournal:  1,
		RecoveryRuns:     2,
		RecoveryRoutes:   3,
		RecoveryPermits:  4,
		RecoveryPressure: 5,
	}
	out := make([]RecoveryStep, 0, len(p.steps))
	for _, step := range p.steps {
		step.Requires = append([]RecoveryStage(nil), step.Requires...)
		out = append(out, step)
	}
	sort.Slice(out, func(i, j int) bool { return order[out[i].Stage] < order[out[j].Stage] })
	return out
}

func (p *RecoveryPlan) Validate() error {
	if len(p.steps) != 5 {
		return errors.New("recovery plan must contain five stages")
	}
	pressure, ok := p.steps[RecoveryPressure]
	if !ok {
		return errors.New("recovery plan has no pressure stage")
	}
	routeDependency := false
	permitDependency := false
	for _, dependency := range pressure.Requires {
		if dependency == RecoveryRoutes {
			routeDependency = true
		}
		if dependency == RecoveryPermits {
			permitDependency = true
		}
	}
	if !routeDependency || !permitDependency {
		return errors.New("pressure recovery lacks route or permit fencing")
	}
	return nil
}

func ValidateRecoveryOrdering() error {
	plan := NewRecoveryPlan()
	if err := plan.Validate(); err != nil {
		return err
	}
	for _, stage := range canonicalRecoveryOrder {
		if !plan.Ready(stage) {
			return fmt.Errorf("recovery stage %s is not ready in canonical order", stage)
		}
		if err := plan.Complete(stage); err != nil {
			return err
		}
	}
	if !plan.Completed(RecoveryPressure) {
		return errors.New("pressure recovery did not complete")
	}
	return nil
}
