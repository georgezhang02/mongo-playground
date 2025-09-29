package replset

import (
	"testing"
	"time"
)

func TestReplicaSetLifecycleWithMongosh(t *testing.T) {
	ports := []string{"27017", "27018", "27019"}

	// Start 3 mongod instances
	rs := NewReplset(ports)

	// Initiate replica set from the first node
	rs.Start(false)

	// Give some time for election
	time.Sleep(5 * time.Second)

	// Check replica set status
	status, err := rs.Status("27017")
	if err != nil {
		rs.Stop()
		t.Fatal(err)
	}

	members, ok := status["members"].([]any)
	if !ok || len(members) != 3 {
		rs.Stop()
		t.Fatalf("expected 3 members, got %+v", status["members"])
	}

	rs.Stop()
}
