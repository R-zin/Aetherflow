package sdk

import (
	"errors"
	"fmt"
)

type CompensationStep struct {
	Name   string
	Action func(ctx WorkflowContext) error
}

type Saga struct {
	steps []*CompensationStep
}

func NewSaga() *Saga {
	return &Saga{
		steps: make([]*CompensationStep, 0),
	}
}

func (s *Saga) AddCompensation(name string, fn func(ctx WorkflowContext) error) {
	s.steps = append(s.steps, &CompensationStep{
		Name:   name,
		Action: fn,
	})
}

func (s *Saga) Compensate(ctx WorkflowContext) error {
	var errs []error
	// Execute compensations in LIFO (reverse) order
	for i := len(s.steps) - 1; i >= 0; i-- {
		step := s.steps[i]
		if err := step.Action(ctx); err != nil {
			errs = append(errs, fmt.Errorf("compensation step %s failed: %w", step.Name, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Saga) CompensateOnError(ctx WorkflowContext, err error) error {
	if err == nil || errors.Is(err, ErrReplayYield) {
		return nil
	}
	return s.Compensate(ctx)
}

func (s *Saga) StepsCount() int {
	return len(s.steps)
}
