package main

import (
	"aigate"
	"fmt"
	"os"
)

func main() {
	if err := aigate.RunSwitch(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "aigate-switch:", err)
		os.Exit(1)
	}
}
