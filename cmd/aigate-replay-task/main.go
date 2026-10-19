package main

import (
	"aigate"
	"fmt"
	"os"
)

func main() {
	if err := aigate.RunReplayTask(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
