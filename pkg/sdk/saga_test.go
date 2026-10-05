package sdk_test

import (
	"errors"
	"testing"

	"github.com/R-zin/Aetherflow/pkg/sdk"
)

func TestSaga_ReverseUnwinding(t *testing.T) {
	saga := sdk.NewSaga()

	executionOrder := make([]string, 0)

	saga.AddCompensation("CancelFlight", func(ctx sdk.WorkflowContext) error {
		executionOrder = append(executionOrder, "CancelFlight")
		return nil
	})

	saga.AddCompensation("CancelHotel", func(ctx sdk.WorkflowContext) error {
		executionOrder = append(executionOrder, "CancelHotel")
		return nil
	})

	saga.AddCompensation("CancelCar", func(ctx sdk.WorkflowContext) error {
		executionOrder = append(executionOrder, "CancelCar")
		return nil
	})

	if saga.StepsCount() != 3 {
		t.Fatalf("expected 3 steps, got %d", saga.StepsCount())
	}

	err := saga.Compensate(nil)
	if err != nil {
		t.Fatalf("unexpected error during compensation: %v", err)
	}

	// Must be reverse order: CancelCar -> CancelHotel -> CancelFlight
	expected := []string{"CancelCar", "CancelHotel", "CancelFlight"}
	if len(executionOrder) != len(expected) {
		t.Fatalf("expected %d compensations, got %d", len(expected), len(executionOrder))
	}
	for i := range expected {
		if executionOrder[i] != expected[i] {
			t.Fatalf("expected step %d to be %s, got %s", i, expected[i], executionOrder[i])
		}
	}
}

func TestSaga_ErrorAggregation(t *testing.T) {
	saga := sdk.NewSaga()

	saga.AddCompensation("Step1", func(ctx sdk.WorkflowContext) error {
		return errors.New("err1")
	})
	saga.AddCompensation("Step2", func(ctx sdk.WorkflowContext) error {
		return errors.New("err2")
	})

	err := saga.Compensate(nil)
	if err == nil {
		t.Fatalf("expected error from failed compensations")
	}
}
