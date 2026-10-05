package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		printHelp()
		os.Exit(1)
	}

	serverAddr := os.Getenv("AETHERFLOW_SERVER")
	if serverAddr == "" {
		serverAddr = "127.0.0.1:7233"
	}

	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to AetherFlow server at %s: %v", serverAddr, err)
	}
	defer conn.Close()

	client := v1.NewWorkflowServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch os.Args[1] {
	case "workflow":
		handleWorkflowCmd(ctx, client, os.Args[2:])
	case "cluster":
		handleClusterCmd(ctx, client, os.Args[2:])
	default:
		printHelp()
		os.Exit(1)
	}
}

func printHelp() {
	fmt.Println("AetherFlow CLI (aetherctl) - Durable Distributed Workflow Engine")
	fmt.Println("\nUsage:")
	fmt.Println("  aetherctl workflow start --type <type> [--id <wfid>] [--queue <queue>] [--input <json>]")
	fmt.Println("  aetherctl workflow show --id <wfid>")
	fmt.Println("  aetherctl cluster status")
}

func handleWorkflowCmd(ctx context.Context, client v1.WorkflowServiceClient, args []string) {
	if len(args) == 0 {
		printHelp()
		os.Exit(1)
	}

	switch args[0] {
	case "start":
		fs := flag.NewFlagSet("start", flag.ExitOnError)
		wfType := fs.String("type", "", "Workflow type")
		wfID := fs.String("id", "", "Workflow ID")
		queue := fs.String("queue", "default", "Task queue")
		input := fs.String("input", "{}", "JSON input payload")
		_ = fs.Parse(args[1:])

		if *wfType == "" {
			fmt.Println("Error: --type is required")
			os.Exit(1)
		}

		resp, err := client.StartWorkflowExecution(ctx, &v1.StartWorkflowRequest{
			WorkflowId:   *wfID,
			WorkflowType: *wfType,
			TaskQueue:    *queue,
			InputPayload: []byte(*input),
		})
		if err != nil {
			log.Fatalf("Failed to start workflow: %v", err)
		}
		fmt.Printf("✓ Workflow started successfully!\nWorkflowID: %s\nRunID:      %s\n", resp.WorkflowId, resp.RunId)

	case "show":
		fs := flag.NewFlagSet("show", flag.ExitOnError)
		wfID := fs.String("id", "", "Workflow ID")
		_ = fs.Parse(args[1:])

		if *wfID == "" {
			fmt.Println("Error: --id is required")
			os.Exit(1)
		}

		resp, err := client.GetWorkflowExecutionHistory(ctx, &v1.GetHistoryRequest{
			WorkflowId: *wfID,
		})
		if err != nil {
			log.Fatalf("Failed to get workflow history: %v", err)
		}

		fmt.Printf("Workflow: %s | Status: %s\n", *wfID, resp.Status)
		fmt.Printf("%-5s | %-25s | %-32s | %s\n", "ID", "TIMESTAMP", "EVENT TYPE", "DETAILS")
		fmt.Println("------------------------------------------------------------------------------------------------------")
		for _, e := range resp.History.Events {
			tStr := time.Unix(0, e.TimestampNs).Format("15:04:05.000")
			details := ""
			switch e.EventType {
			case v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
				attrs := e.GetWorkflowExecutionStartedAttributes()
				details = fmt.Sprintf("Type: %s, Queue: %s", attrs.WorkflowType, attrs.TaskQueue)
			case v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
				attrs := e.GetActivityTaskScheduledAttributes()
				details = fmt.Sprintf("ActivityID: %s, Type: %s", attrs.ActivityId, attrs.ActivityType)
			case v1.EventType_EVENT_TYPE_ACTIVITY_TASK_STARTED:
				attrs := e.GetActivityTaskStartedAttributes()
				details = fmt.Sprintf("ActivityID: %s, Worker: %s", attrs.ActivityId, attrs.WorkerIdentity)
			case v1.EventType_EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
				attrs := e.GetActivityTaskCompletedAttributes()
				details = fmt.Sprintf("ActivityID: %s, Result: %s", attrs.ActivityId, string(attrs.ResultPayload))
			case v1.EventType_EVENT_TYPE_ACTIVITY_TASK_FAILED:
				attrs := e.GetActivityTaskFailedAttributes()
				details = fmt.Sprintf("ActivityID: %s, Error: %s", attrs.ActivityId, attrs.ErrorMessage)
			case v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
				attrs := e.GetWorkflowExecutionCompletedAttributes()
				details = fmt.Sprintf("Result: %s", string(attrs.ResultPayload))
			case v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
				attrs := e.GetWorkflowExecutionFailedAttributes()
				details = fmt.Sprintf("Error: %s", attrs.ErrorMessage)
			}
			fmt.Printf("%-5d | %-25s | %-32s | %s\n", e.EventId, tStr, e.EventType.String(), details)
		}

	default:
		printHelp()
	}
}

func handleClusterCmd(ctx context.Context, client v1.WorkflowServiceClient, args []string) {
	if len(args) == 0 || args[0] != "status" {
		printHelp()
		os.Exit(1)
	}

	resp, err := client.GetClusterStatus(ctx, &v1.GetClusterStatusRequest{})
	if err != nil {
		log.Fatalf("Failed to fetch cluster status: %v", err)
	}

	fmt.Println("Active Cluster Nodes:")
	for _, n := range resp.ActiveNodes {
		fmt.Printf("  • %s\n", n)
	}
	fmt.Printf("\nActive Shard Leases: %d\n", len(resp.ShardAssignments))
	for i, sa := range resp.ShardAssignments {
		if i < 10 {
			fmt.Printf("  Shard #%-4d -> Owner: %-15s (Epoch: %d)\n", sa.ShardId, sa.OwnerNodeId, sa.Epoch)
		}
	}
	if len(resp.ShardAssignments) > 10 {
		fmt.Printf("  ... and %d more shards\n", len(resp.ShardAssignments)-10)
	}
}
