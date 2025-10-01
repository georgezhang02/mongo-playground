package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

// Wire protocol constants
const (
	// Message types
	OpReply        = 1
	OpUpdate       = 2001
	OpInsert       = 2002
	OpQuery        = 2004
	OpGetMore      = 2005
	OpDelete       = 2006
	OpKillCursors  = 2007
	OpCommand      = 2010
	OpCommandReply = 2011
	OpMsg          = 2013

	// Header size
	HeaderSize = 16
)

// MessageHeader represents the MongoDB wire protocol message header
type MessageHeader struct {
	MessageLength int32
	RequestID     int32
	ResponseTo    int32
	OpCode        int32
}

// Replset represents a MongoDB replica set with multiple nodes
type Replset struct {
	nodes     []string
	conns     map[string]net.Conn
	mu        sync.RWMutex
	requestID int32
}

// NewReplset creates a new replica set abstraction
func NewReplset(nodes []string) *Replset {
	return &Replset{
		nodes: nodes,
		conns: make(map[string]net.Conn),
	}
}

// Connect establishes connections to all replica set nodes
func (r *Replset) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, node := range r.nodes {
		conn, err := net.DialTimeout("tcp", node, 5*time.Second)
		if err != nil {
			// Close any already established connections
			r.closeConnections()
			return fmt.Errorf("failed to connect to %s: %w", node, err)
		}
		r.conns[node] = conn
	}

	return nil
}

// Disconnect closes all connections
func (r *Replset) Disconnect() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeConnections()
}

// SendMessage sends a MongoDB wire protocol message to the primary node
func (r *Replset) SendMessage(ctx context.Context, opCode int32, payload []byte) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.conns) == 0 {
		return nil, fmt.Errorf("no connections available")
	}

	// For simplicity, use the first available connection
	// In a real implementation, you'd want to determine the primary
	var conn net.Conn
	for _, c := range r.conns {
		conn = c
		break
	}

	// Generate request ID
	r.requestID++
	requestID := r.requestID

	// Send request
	err := sendRequest(conn, opCode, requestID, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Read response
	response, err := readResponse(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return response, nil
}

// SendQuery sends a query message using OP_MSG with kind 0 body section
func (r *Replset) SendQuery(ctx context.Context, query []byte) ([]byte, error) {
	// Build OP_MSG payload with kind 0 body section
	var payload bytes.Buffer

	// Flag bits (4 bytes) - 0 for normal message
	binary.Write(&payload, binary.LittleEndian, uint32(0))

	// Kind 0: Body section
	payload.WriteByte(0)

	// Write the query document (should include database and collection)
	// For example: {"find": "collection", "filter": {...}, "$db": "database"}
	payload.Write(query)

	return r.SendMessage(ctx, OpMsg, payload.Bytes())
}

// SendCommand sends a command message using OP_MSG with kind 0 body section and optional kind 1 document sequence
func (r *Replset) SendCommand(ctx context.Context, command []byte, documents [][]byte) ([]byte, error) {
	// Build OP_MSG payload with kind 0 body section and optional kind 1 document sequence
	var payload bytes.Buffer

	// Flag bits (4 bytes) - 0 for normal message
	binary.Write(&payload, binary.LittleEndian, uint32(0))

	// Kind 0: Body section
	payload.WriteByte(0)

	// Write the command document (should include database)
	// For example: {"insert": "collection", "$db": "database"}
	payload.Write(command)

	// Kind 1: Document sequence (if documents provided)
	if len(documents) > 0 {
		payload.WriteByte(1) // Kind 1

		// Calculate total size of document sequence
		// Size includes: size field (4 bytes) + identifier + all documents
		var totalSize int32 = 4 // size field itself

		// Add identifier (null-terminated string)
		identifier := "documents"               // Default identifier for document sequence
		totalSize += int32(len(identifier) + 1) // +1 for null terminator

		// Add size of all documents
		for _, doc := range documents {
			totalSize += int32(len(doc))
		}

		// Write size of the section
		binary.Write(&payload, binary.LittleEndian, totalSize)

		// Write identifier (null-terminated)
		payload.WriteString(identifier)
		payload.WriteByte(0)

		// Write all documents
		for _, doc := range documents {
			payload.Write(doc)
		}
	}

	return r.SendMessage(ctx, OpMsg, payload.Bytes())
}

// serializeHeader converts a MessageHeader to its wire protocol representation
func serializeHeader(header MessageHeader) []byte {
	headerBytes := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(headerBytes[0:4], uint32(header.MessageLength))
	binary.LittleEndian.PutUint32(headerBytes[4:8], uint32(header.RequestID))
	binary.LittleEndian.PutUint32(headerBytes[8:12], uint32(header.ResponseTo))
	binary.LittleEndian.PutUint32(headerBytes[12:16], uint32(header.OpCode))
	return headerBytes
}

// sendRequest creates a message header and sends the request
func sendRequest(conn net.Conn, opCode, requestID int32, payload []byte) error {
	// Create message header
	header := MessageHeader{
		MessageLength: int32(HeaderSize + len(payload)),
		RequestID:     requestID,
		ResponseTo:    0,
		OpCode:        opCode,
	}

	// Serialize header
	headerBytes := serializeHeader(header)

	// Send message
	message := append(headerBytes, payload...)
	_, err := conn.Write(message)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// readResponse reads a response from the connection
func readResponse(conn net.Conn) ([]byte, error) {
	// Read header
	headerBytes := make([]byte, HeaderSize)
	_, err := conn.Read(headerBytes)
	if err != nil {
		return nil, err
	}

	// Parse header
	messageLength := binary.LittleEndian.Uint32(headerBytes[0:4])
	requestID := binary.LittleEndian.Uint32(headerBytes[4:8])
	responseTo := binary.LittleEndian.Uint32(headerBytes[8:12])
	opCode := binary.LittleEndian.Uint32(headerBytes[12:16])

	// Read payload
	payloadSize := int(messageLength) - HeaderSize
	if payloadSize < 0 {
		return nil, fmt.Errorf("invalid message length: %d", messageLength)
	}

	payload := make([]byte, payloadSize)
	_, err = conn.Read(payload)
	if err != nil {
		return nil, err
	}

	// Log response details for debugging
	_ = requestID
	_ = responseTo
	_ = opCode

	return payload, nil
}

// closeConnections closes all connections
func (r *Replset) closeConnections() error {
	var lastErr error
	for node, conn := range r.conns {
		if err := conn.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close connection to %s: %w", node, err)
		}
	}
	r.conns = make(map[string]net.Conn)
	return lastErr
}

// GetNodes returns the list of nodes in the replica set
func (r *Replset) GetNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, len(r.nodes))
	copy(nodes, r.nodes)
	return nodes
}

// IsConnected checks if the replica set is connected
func (r *Replset) IsConnected() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.conns) > 0
}
