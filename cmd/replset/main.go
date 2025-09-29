package main

import (
	"fmt"
	"mongo-playground/internal/replset"
	"os"
)

func main() {
	ports := []string{"27105", "27106", "27107"}
	rs := replset.NewReplset(ports)

	args := os.Args[1:]
	if len(args) == 0 {
		rs.Start(false)
		return
	}

	switch args[0] {
	case "--persistent":
		rs.Start(true)
	case "stop":
		rs.Stop()
	default:
		fmt.Println("Usage: replset [--persistent|stop]")
	}
}
