package main

import (
	"aigate"
	"fmt"
	"os"
)

func main() {
	if err := aigate.RunProxy(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "aigate-proxy:", err)
		os.Exit(1)
	}
}
