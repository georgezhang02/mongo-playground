package replset

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Start brings up the replica set.
func (rs *Replset) Start(persistent bool) {
	for i, port := range rs.ports {
		if err := startMongod(fmt.Sprintf("rs0-%v", i), port, persistent); err != nil {
			rs.Stop()
			log.Fatal(err)
		}
	}
	err := initReplicaSet(rs.ports)
	if err != nil {
		rs.Stop()
		log.Fatal(err)
	}
}

func startMongod(instance, port string, persistent bool) error {
	dbpath := filepath.Join("data", instance)
	if !persistent {
		// wipe the dbpath each run
		os.RemoveAll(dbpath)
	}
	if err := os.MkdirAll(dbpath, 0755); err != nil {
		log.Fatalf("failed to create dbpath %s: %v", dbpath, err)
	}

	args := []string{
		"--replSet", "rs0",
		"--port", port,
		"--dbpath", dbpath,
		"--bind_ip", "127.0.0.1",
		"--fork",
		"--logpath", filepath.Join(dbpath, "mongod.log"),
	}
	fmt.Println("Starting mongod:", args)
	err := runCommand("mongod", args...)
	if err != nil {
		return fmt.Errorf("failed to start mongod: %w", err)
	}
	return nil
}

func initReplicaSet(ports []string) error {

	memberStrings := make([]string, 0, len(ports))
	for i, port := range ports {
		memberStrings = append(memberStrings, fmt.Sprintf(`{ _id: %d, host: "127.0.0.1:%v" }`, i, port))
	}

	initJS := fmt.Sprintf(`
rs.initiate({
  _id: "rs0",
  members: [%v]
})
`, strings.Join(memberStrings, ", "))
	return runCommand("mongosh", "--port", ports[0], "--eval", initJS)
}
