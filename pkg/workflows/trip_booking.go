package workflows

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/R-zin/Aetherflow/pkg/sdk"
)

type TripBookingRequest struct {
	CustomerID string `json:"customer_id"`
	FailCar    bool   `json:"fail_car"`
}

type BookingResult struct {
	ConfirmationID string `json:"confirmation_id"`
	Status         string `json:"status"`
}

func init() {
	sdk.RegisterWorkflow("TripBookingWorkflow", func(ctx sdk.WorkflowContext, input []byte) ([]byte, error) {
		var req TripBookingRequest
		if len(input) > 0 {
			_ = json.Unmarshal(input, &req)
		}

		saga := sdk.NewSaga()

		// 1. Book Flight
		var flightRes BookingResult
		log.Printf("[Workflow %s] Step 1: Executing BookFlightActivity...", ctx.WorkflowID())
		err := ctx.ExecuteActivity("BookFlightActivity", req, &flightRes, sdk.WithActivityID("book-flight"), sdk.WithTaskQueue("trip-queue"))
		if err != nil {
			return nil, err
		}

		// Register compensation for flight
		saga.AddCompensation("CancelFlight", func(c sdk.WorkflowContext) error {
			var cancelRes BookingResult
			log.Printf("[Saga Compensation %s] Executing CancelFlightActivity...", c.WorkflowID())
			return c.ExecuteActivity("CancelFlightActivity", flightRes, &cancelRes, sdk.WithActivityID("cancel-flight"), sdk.WithTaskQueue("trip-queue"))
		})

		// 2. Book Hotel
		var hotelRes BookingResult
		log.Printf("[Workflow %s] Step 2: Executing BookHotelActivity...", ctx.WorkflowID())
		err = ctx.ExecuteActivity("BookHotelActivity", req, &hotelRes, sdk.WithActivityID("book-hotel"), sdk.WithTaskQueue("trip-queue"))
		if err != nil {
			if !errors.Is(err, sdk.ErrReplayYield) {
				log.Printf("[Workflow %s] Hotel failed: %v. Compensating...", ctx.WorkflowID(), err)
				_ = saga.Compensate(ctx)
			}
			return nil, err
		}

		// Register compensation for hotel
		saga.AddCompensation("CancelHotel", func(c sdk.WorkflowContext) error {
			var cancelRes BookingResult
			log.Printf("[Saga Compensation %s] Executing CancelHotelActivity...", c.WorkflowID())
			return c.ExecuteActivity("CancelHotelActivity", hotelRes, &cancelRes, sdk.WithActivityID("cancel-hotel"), sdk.WithTaskQueue("trip-queue"))
		})

		// 3. Book Car Rental
		var carRes BookingResult
		log.Printf("[Workflow %s] Step 3: Executing BookCarActivity...", ctx.WorkflowID())
		err = ctx.ExecuteActivity("BookCarActivity", req, &carRes, sdk.WithActivityID("book-car"), sdk.WithTaskQueue("trip-queue"))
		if err != nil {
			if !errors.Is(err, sdk.ErrReplayYield) {
				log.Printf("[Workflow %s] Car rental failed: %v. Initiating Saga compensations!", ctx.WorkflowID(), err)
				compErr := saga.Compensate(ctx)
				if compErr != nil && !errors.Is(compErr, sdk.ErrReplayYield) {
					return nil, fmt.Errorf("car rental failed (%v); compensation error: %w", err, compErr)
				}
				return nil, fmt.Errorf("car rental booking failed; compensated prior reservations: %w", err)
			}
			return nil, err
		}

		log.Printf("[Workflow %s] Entire trip booked successfully!", ctx.WorkflowID())
		return json.Marshal(map[string]any{
			"status": "CONFIRMED",
			"flight": flightRes.ConfirmationID,
			"hotel":  hotelRes.ConfirmationID,
			"car":    carRes.ConfirmationID,
		})
	})
}
