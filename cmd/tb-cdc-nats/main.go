package main

import (
	"os"

	cdcnats "github.com/stumct/tigerbeetle-cdc-nats"
)

var buildVersion = "dev"

func main() {
	os.Exit(cdcnats.RunCLI(os.Args[1:], buildVersion))
}
