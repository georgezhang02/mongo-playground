package client

import (
	"context"

	pb "mongo-playground/proto/proxy"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client provides a thin wrapper around the generated gRPC client.
type Client struct {
	conn   *grpc.ClientConn
	client pb.MongoProxyClient
}

// Connect dials a gRPC server at target, e.g., "localhost:50051".
func NewClient(target string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())} // for local dev; consider TLS for production
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	client := pb.NewMongoProxyClient(conn)
	return &Client{conn: conn, client: client}, nil
}

// Close closes the underlying client connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Insert forwards an Insert RPC.
func (c *Client) Insert(ctx context.Context, req *pb.InsertRequest, opts ...grpc.CallOption) (*pb.InsertResponse, error) {
	return c.client.Insert(ctx, req, opts...)
}

// Find forwards a Find RPC.
func (c *Client) Find(ctx context.Context, req *pb.FindRequest, opts ...grpc.CallOption) (*pb.FindResponse, error) {
	return c.client.Find(ctx, req, opts...)
}
