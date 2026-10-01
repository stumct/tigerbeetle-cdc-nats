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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// After the first signal, restore default handling so a second signal exits immediately.
	context.AfterFunc(ctx, func() {
		stop()
		log.Printf("shutdown requested; stopping (signal again to exit immediately)")
	})

	if err := run(ctx, cfg, openTigerBeetle); err != nil {
		log.Printf("error: %v", err)
		return 1
	}

	return 0
}
