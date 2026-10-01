package cdcnats

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/nats-io/nats.go"
)

// connectNATS connects with the configured credentials and TLS files, and logs connection state
// changes without credentials.
func connectNATS(cfg config) (*nats.Conn, error) {
	options := []nats.Option{
		nats.Name("tb-cdc-nats"),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Printf("warning: disconnected from NATS: %v", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			// ConnectedUrlRedacted masks passwords but not tokens.
			log.Printf("reconnected to NATS at %s", redactURLs(nc.ConnectedUrl()))
		}),
	}

	if cfg.natsCredsFile != "" {
		options = append(options, nats.UserCredentials(cfg.natsCredsFile))
	}

	if cfg.natsNKeyFile != "" {
		option, err := nats.NkeyOptionFromSeed(cfg.natsNKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load --nats-nkey: %w", err)
		}
		options = append(options, option)
	}

	if cfg.natsTLSCAFile != "" {
		options = append(options, nats.RootCAs(cfg.natsTLSCAFile))
	}

	if cfg.natsTLSCertFile != "" {
		options = append(options, nats.ClientCert(cfg.natsTLSCertFile, cfg.natsTLSKeyFile))
	}

	nc, err := nats.Connect(cfg.natsURL, options...)
	if err != nil {
		// A URL parse error quotes the whole URL, credentials included. Keep only the reason.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = fmt.Errorf("invalid URL: %w", urlErr.Err)
		}
		return nil, fmt.Errorf("connect to NATS at %s: %w", redactURLs(cfg.natsURL), err)
	}
	return nc, nil
}

// redactURLs removes credentials (user and password, or a token) from a comma-separated list of
// NATS URLs so the list can be logged.
func redactURLs(urls string) string {
	parts := strings.Split(urls, ",")
	for i, part := range parts {
		at := strings.LastIndex(part, "@")
		if at < 0 {
			continue
		}

		scheme := ""
		if separator := strings.Index(part, "://"); separator >= 0 && separator < at {
			scheme = part[:separator+len("://")]
		}
		parts[i] = scheme + "[redacted]@" + part[at+1:]
	}
	return strings.Join(parts, ",")
}
