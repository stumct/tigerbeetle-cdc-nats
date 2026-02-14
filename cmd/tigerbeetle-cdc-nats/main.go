package main

import (
	"os"

	cdcnats "github.com/tigerbeetle/tigerbeetle-cdc-nats"
)

var buildVersion = "dev"

func main() {
	os.Exit(cdcnats.RunCLI(os.Args[1:], buildVersion))
}
