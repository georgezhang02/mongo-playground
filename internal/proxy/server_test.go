package proxy

import (
	"context"
	"testing"
)

func TestServer_Insert_Direct(t *testing.T) {
	s := NewServer()
	resp, err := s.Insert(context.Background(), nil)
	if err != nil {
		t.Fatalf("Insert returned error: %v", err)
	}
	if resp == nil || !resp.Success {
		t.Fatalf("unexpected Insert response: %#v", resp)
	}
}

func TestServer_Find_Direct(t *testing.T) {
	s := NewServer()
	resp, err := s.Find(context.Background(), nil)
	if err != nil {
		t.Fatalf("Find returned error: %v", err)
	}
	if resp == nil {
		t.Fatalf("nil Find response")
	}
}
