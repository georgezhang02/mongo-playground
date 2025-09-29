package replset

import "fmt"

// Stop shuts down the mongod processes cleanly.
func (rs *Replset) Stop() {
	for _, port := range rs.ports {
		fmt.Printf("Stopping mongod on port %s...\n", port)
		err := runCommand("mongosh", "--port", port, "--eval", "db.adminCommand({ \"shutdown\" : 1, \"force\" : true})")
		if err != nil {
			fmt.Println(err)
		}
	}
}
