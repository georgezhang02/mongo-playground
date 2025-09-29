package replset

import (
	"encoding/json"
	"fmt"
)

// Start brings up the replica set.
func (rs *Replset) Status(port string) (map[string]any, error) {
	out, err := runCommandWithOutput("mongosh", "--port", port, "--eval", "JSON.stringify(rs.status())")
	if err != nil {
		return nil, fmt.Errorf("failed to run replset status: %w", err)
	}

	var result map[string]any
	if err := json.Unmarshal(out, &result); err != nil {
		return nil, fmt.Errorf("failed to parse replset status JSON output: %w\n%s", err, string(out))
	}
	return result, nil
}
