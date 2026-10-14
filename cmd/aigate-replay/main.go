package main

import (
	"aigate"
	"fmt"
	"os"
)

func main() {
	if err := aigate.RunReplay(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "aigate-replay:", err)
		os.Exit(1)
	}
}
