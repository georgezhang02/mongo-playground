package proxy

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// mockMongoServer simulates a MongoDB server for testing
type mockMongoServer struct {
	listener net.Listener
	conns    []net.Conn
}

func newMockMongoServer() (*mockMongoServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	server := &mockMongoServer{
		listener: listener,
		conns:    make([]net.Conn, 0),
	}

	go server.acceptConnections()

	return server, nil
}

func (m *mockMongoServer) acceptConnections() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			return
		}
		m.conns = append(m.conns, conn)
		go m.handleConnection(conn)
	}
}

func (m *mockMongoServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read the incoming message
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return
	}

	// Parse the header
	if n < 16 {
		return
	}

	messageLength := binary.LittleEndian.Uint32(buffer[0:4])
	requestID := binary.LittleEndian.Uint32(buffer[4:8])
	responseTo := binary.LittleEndian.Uint32(buffer[8:12])
	opCode := binary.LittleEndian.Uint32(buffer[12:16])

	// Create a mock response with payload
	payload := []byte(`{"ok": 1}`)
	responseHeader := make([]byte, 16)
	binary.LittleEndian.PutUint32(responseHeader[0:4], uint32(16+len(payload))) // Message length (header + payload)
	binary.LittleEndian.PutUint32(responseHeader[4:8], 1)                       // Response request ID
	binary.LittleEndian.PutUint32(responseHeader[8:12], requestID)              // Response to original request
	binary.LittleEndian.PutUint32(responseHeader[12:16], opCode)                // Same op code

	// Send response (header + payload)
	conn.Write(responseHeader)
	conn.Write(payload)

	_ = messageLength
	_ = responseTo
}

func (m *mockMongoServer) Addr() string {
	return m.listener.Addr().String()
}

func (m *mockMongoServer) Close() error {
	for _, conn := range m.conns {
		conn.Close()
	}
	return m.listener.Close()
}

func TestNewReplset(t *testing.T) {
	nodes := []string{"127.0.0.1:27017", "127.0.0.1:27018"}
	replset := NewReplset(nodes)

	if replset == nil {
		t.Fatal("NewReplset returned nil")
	}

	actualNodes := replset.GetNodes()
	if len(actualNodes) != len(nodes) {
		t.Errorf("Expected %d nodes, got %d", len(nodes), len(actualNodes))
	}

	for i, expected := range nodes {
		if actualNodes[i] != expected {
			t.Errorf("Expected node %d to be %s, got %s", i, expected, actualNodes[i])
		}
	}

	if replset.IsConnected() {
		t.Error("Expected replset to not be connected initially")
	}
}

func TestReplsetConnect(t *testing.T) {
	// Start mock server
	server, err := newMockMongoServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	// Create replset with mock server address
	replset := NewReplset([]string{server.Addr()})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = replset.Connect(ctx)
	if err != nil {
		t.Errorf("Connect failed: %v", err)
	}

	if !replset.IsConnected() {
		t.Error("Expected replset to be connected after Connect()")
	}

	// Test disconnect
	err = replset.Disconnect()
	if err != nil {
		t.Errorf("Disconnect failed: %v", err)
	}

	if replset.IsConnected() {
		t.Error("Expected replset to not be connected after Disconnect()")
	}
}

func TestReplsetConnectFailure(t *testing.T) {
	// Create replset with invalid address
	replset := NewReplset([]string{"127.0.0.1:99999"})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := replset.Connect(ctx)
	if err == nil {
		t.Error("Expected Connect to fail with invalid address")
	}

	if replset.IsConnected() {
		t.Error("Expected replset to not be connected after failed Connect()")
	}
}

func TestReplsetSendMessage(t *testing.T) {
	// Start mock server
	server, err := newMockMongoServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	// Create replset with mock server address
	replset := NewReplset([]string{server.Addr()})

	// Connect
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = replset.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Test sending a message
	payload := []byte("test payload")
	response, err := replset.SendMessage(ctx, OpQuery, payload)
	if err != nil {
		t.Errorf("SendMessage failed: %v", err)
	}

	if len(response) == 0 {
		t.Error("Expected non-empty response")
	}
}

func TestReplsetSendQuery(t *testing.T) {
	// Start mock server
	server, err := newMockMongoServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	// Create replset with mock server address
	replset := NewReplset([]string{server.Addr()})

	// Connect
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = replset.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Test sending a query
	query := []byte(`{"find": "users", "$db": "testdb"}`)
	response, err := replset.SendQuery(ctx, query)
	if err != nil {
		t.Errorf("SendQuery failed: %v", err)
	}

	if len(response) == 0 {
		t.Error("Expected non-empty response")
	}
}

func TestReplsetSendCommand(t *testing.T) {
	// Start mock server
	server, err := newMockMongoServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	// Create replset with mock server address
	replset := NewReplset([]string{server.Addr()})

	// Connect
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = replset.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Test sending a command
	command := []byte(`{"ping": 1, "$db": "testdb"}`)
	response, err := replset.SendCommand(ctx, command, nil)
	if err != nil {
		t.Errorf("SendCommand failed: %v", err)
	}

	if len(response) == 0 {
		t.Error("Expected non-empty response")
	}
}

func TestReplsetSendCommandWithDocuments(t *testing.T) {
	// Start mock server
	server, err := newMockMongoServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer server.Close()

	// Create replset with mock server address
	replset := NewReplset([]string{server.Addr()})

	// Connect
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = replset.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Test sending a command with documents (kind 1 section)
	command := []byte(`{"insert": "users", "$db": "testdb"}`)
	documents := [][]byte{
		[]byte(`{"name": "John", "age": 30}`),
		[]byte(`{"name": "Jane", "age": 25}`),
	}
	response, err := replset.SendCommand(ctx, command, documents)
	if err != nil {
		t.Errorf("SendCommand with documents failed: %v", err)
	}

	if len(response) == 0 {
		t.Error("Expected non-empty response")
	}
}

func TestReplsetSendMessageWithoutConnection(t *testing.T) {
	replset := NewReplset([]string{"127.0.0.1:27017"})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	payload := []byte("test payload")
	_, err := replset.SendMessage(ctx, OpQuery, payload)
	if err == nil {
		t.Error("Expected SendMessage to fail without connection")
	}
}

func TestReplsetMultipleNodes(t *testing.T) {
	// Start multiple mock servers
	servers := make([]*mockMongoServer, 3)
	addrs := make([]string, 3)

	for i := 0; i < 3; i++ {
		server, err := newMockMongoServer()
		if err != nil {
			t.Fatalf("Failed to start mock server %d: %v", i, err)
		}
		servers[i] = server
		addrs[i] = server.Addr()
	}

	// Clean up servers
	defer func() {
		for _, server := range servers {
			server.Close()
		}
	}()

	// Create replset with multiple nodes
	replset := NewReplset(addrs)

	// Connect
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := replset.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if !replset.IsConnected() {
		t.Error("Expected replset to be connected")
	}

	// Test that we can send messages
	payload := []byte("test payload")
	response, err := replset.SendMessage(ctx, OpQuery, payload)
	if err != nil {
		t.Errorf("SendMessage failed: %v", err)
	}

	if len(response) == 0 {
		t.Error("Expected non-empty response")
	}
}

func TestMessageHeaderSerialization(t *testing.T) {
	header := MessageHeader{
		MessageLength: 100,
		RequestID:     1,
		ResponseTo:    0,
		OpCode:        OpQuery,
	}

	// Serialize header
	headerBytes := serializeHeader(header)

	// Deserialize header
	messageLength := binary.LittleEndian.Uint32(headerBytes[0:4])
	requestID := binary.LittleEndian.Uint32(headerBytes[4:8])
	responseTo := binary.LittleEndian.Uint32(headerBytes[8:12])
	opCode := binary.LittleEndian.Uint32(headerBytes[12:16])

	// Verify
	if messageLength != uint32(header.MessageLength) {
		t.Errorf("Expected MessageLength %d, got %d", header.MessageLength, messageLength)
	}
	if requestID != uint32(header.RequestID) {
		t.Errorf("Expected RequestID %d, got %d", header.RequestID, requestID)
	}
	if responseTo != uint32(header.ResponseTo) {
		t.Errorf("Expected ResponseTo %d, got %d", header.ResponseTo, responseTo)
	}
	if opCode != uint32(header.OpCode) {
		t.Errorf("Expected OpCode %d, got %d", header.OpCode, opCode)
	}
}
