package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/sdk"
	"github.com/R-zin/Aetherflow/pkg/server"
	"github.com/R-zin/Aetherflow/pkg/storage/pebble"
	_ "github.com/R-zin/Aetherflow/pkg/workflows"
	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 7233, "gRPC server port")
	nodeID := flag.String("node-id", "aether-node-1", "Unique node ID")
	dataDir := flag.String("data-dir", "./data/aetherd", "Data directory for persistent storage")
	numShards := flag.Uint("shards", 1024, "Number of virtual shards")
	flag.Parse()

	log.Printf("[AetherFlow] Starting coordinator daemon %s on port %d...", *nodeID, *port)

	absDir, err := filepath.Abs(*dataDir)
	if err != nil {
		log.Fatalf("Invalid data dir: %v", err)
	}
	if err := os.MkdirAll(absDir, 0755); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}

	store, err := pebble.OpenPebbleStore(absDir)
	if err != nil {
		log.Fatalf("Failed to initialize Pebble storage engine: %v", err)
	}
	defer store.Close()

	cfg := server.CoordinatorConfig{
		NodeID:          *nodeID,
		NumShards:       uint32(*numShards),
		LeaseTTL:        5 * time.Second,
		TimerResolution: 20 * time.Millisecond,
	}

	srv := server.NewWorkflowServiceServer(cfg, store, sdk.NewWorkflowRunner(sdk.DefaultRegistry))
	defer srv.Close()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen on tcp port %d: %v", *port, err)
	}

	grpcServer := grpc.NewServer()
	v1.RegisterWorkflowServiceServer(grpcServer, srv)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("[AetherFlow] Shutting down gracefully...")
		grpcServer.GracefulStop()
	}()

	log.Printf("[AetherFlow] Coordinator online and listening at %s", lis.Addr().String())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server error: %v", err)
	}
}
