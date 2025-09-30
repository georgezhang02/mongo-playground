package proxy

import (
	"context"
	"fmt"
	"net"

	pb "mongo-playground/proto/proxy"

	"google.golang.org/grpc"
)

// Server implements pb.MongoProxyServer and hosts the gRPC server.
type Server struct {
	pb.UnimplementedMongoProxyServer

	grpcServer *grpc.Server
}

// NewServer creates a new Server instance.
func NewServer() *Server {
	return &Server{}
}

// Start begins serving on the given address, e.g., ":50051".
func (s *Server) Start(listenAddress string) error {
	if s.grpcServer != nil {
		return fmt.Errorf("server already started")
	}

	ln, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listenAddress, err)
	}

	s.grpcServer = grpc.NewServer()
	pb.RegisterMongoProxyServer(s.grpcServer, s)

	go func() {
		_ = s.grpcServer.Serve(ln)
	}()

	return nil
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
		s.grpcServer = nil
	}
}

// Insert is a placeholder implementation.
func (s *Server) Insert(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	// TODO: Wire to Mongo insert logic
	return &pb.InsertResponse{Success: true}, nil
}

// Find is a placeholder implementation.
func (s *Server) Find(ctx context.Context, req *pb.FindRequest) (*pb.FindResponse, error) {
	// TODO: Wire to Mongo find logic
	return &pb.FindResponse{Documents: nil}, nil
}
