package main

import (
	"os"

	"github.com/GerhardOfRivia/bezorg/internal/cli"
)

// Version is set with -ldflags "-X main.Version=...".
var Version = "dev"

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr, Version))
}
