package client

import (
	"context"
	"testing"
	"time"

	"mongo-playground/internal/proxy"
	pb "mongo-playground/proto/proxy"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestClient_InsertAndFind(t *testing.T) {
	addr := "127.0.0.1:50051"

	// Start the server
	srv := proxy.NewServer()
	srv.Start(addr)
	t.Cleanup(srv.Stop)

	// Create a client
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewMongoProxyClient(conn)

	// Create a context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	// Insert a document
	insResp, err := client.Insert(ctx, &pb.InsertRequest{Db: "test", Collection: "col"})
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}
	if insResp == nil || !insResp.Success {
		t.Fatalf("unexpected Insert response: %#v", insResp)
	}

	// Find a document
	findResp, err := client.Find(ctx, &pb.FindRequest{Db: "test", Collection: "col"})
	if err != nil {
		t.Fatalf("Find error: %v", err)
	}
	if findResp == nil {
		t.Fatalf("nil Find response")
	}
}
