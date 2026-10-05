package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/workflows"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := flag.String("server", "127.0.0.1:7233", "AetherFlow server address")
	workerID := flag.String("worker-id", "trip-worker-1", "Worker identifier")
	taskQueue := flag.String("queue", "trip-queue", "Task queue to listen on")
	flag.Parse()

	log.Printf("[Worker %s] Connecting to coordinator at %s...", *workerID, *serverAddr)
	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := v1.NewWorkflowServiceClient(conn)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	log.Printf("[Worker %s] Polling task queue %q...", *workerID, *taskQueue)

	stop := false
	go func() {
		<-sigCh
		log.Println("Stopping worker...")
		stop = true
	}()

	for !stop {
		resp, err := client.PollActivityTaskQueue(context.Background(), &v1.PollActivityTaskQueueRequest{
			TaskQueue:      *taskQueue,
			WorkerIdentity: *workerID,
			TimeoutSeconds: 3,
		})
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if !resp.HasTask {
			continue
		}

		log.Printf("[Worker %s] Received activity %s (%s) for workflow %s", *workerID, resp.ActivityId, resp.ActivityType, resp.WorkflowId)

		outBytes, actErr := executeActivityLocally(resp.ActivityType, resp.InputPayload)
		if actErr != nil {
			log.Printf("[Worker %s] Activity %s failed: %v", *workerID, resp.ActivityId, actErr)
			_, _ = client.RespondActivityTaskFailed(context.Background(), &v1.RespondActivityFailedRequest{
				WorkflowId:     resp.WorkflowId,
				RunId:          resp.RunId,
				ActivityId:     resp.ActivityId,
				WorkerIdentity: *workerID,
				ErrorMessage:   actErr.Error(),
				NonRetryable:   true,
			})
		} else {
			log.Printf("[Worker %s] Activity %s completed successfully!", *workerID, resp.ActivityId)
			_, _ = client.RespondActivityTaskCompleted(context.Background(), &v1.RespondActivityCompletedRequest{
				WorkflowId:     resp.WorkflowId,
				RunId:          resp.RunId,
				ActivityId:     resp.ActivityId,
				WorkerIdentity: *workerID,
				ResultPayload:  outBytes,
			})
		}
	}
}

func executeActivityLocally(actType string, input []byte) ([]byte, error) {
	var req workflows.TripBookingRequest
	if len(input) > 0 {
		_ = json.Unmarshal(input, &req)
	}

	switch actType {
	case "BookFlightActivity":
		return json.Marshal(workflows.BookingResult{ConfirmationID: "FLIGHT-9988", Status: "CONFIRMED"})
	case "CancelFlightActivity":
		return json.Marshal(workflows.BookingResult{ConfirmationID: "FLIGHT-9988", Status: "CANCELLED"})
	case "BookHotelActivity":
		return json.Marshal(workflows.BookingResult{ConfirmationID: "HOTEL-4433", Status: "CONFIRMED"})
	case "CancelHotelActivity":
		return json.Marshal(workflows.BookingResult{ConfirmationID: "HOTEL-4433", Status: "CANCELLED"})
	case "BookCarActivity":
		if req.FailCar {
			return nil, errors.New("no rental cars available at destination")
		}
		return json.Marshal(workflows.BookingResult{ConfirmationID: "CAR-1122", Status: "CONFIRMED"})
	case "CancelCarActivity":
		return json.Marshal(workflows.BookingResult{ConfirmationID: "CAR-1122", Status: "CANCELLED"})
	default:
		return nil, fmt.Errorf("unknown activity: %s", actType)
	}
}
