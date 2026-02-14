package cdcnats

import (
	"context"
	"errors"
	"flag"
	"log"
	"os/signal"
	"syscall"
)

// RunCLI parses flags, runs CDC, and returns process exit code.
func RunCLI(args []string, version string) int {
	cfg, err := parseConfig(args, version)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		log.Printf("error: %v", err)
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		log.Printf("error: %v", err)
		return 1
	}

	return 0
}
