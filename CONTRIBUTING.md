# Contributing

Thanks for contributing.

## Development

1. Install Go.
2. Run tests:

```bash
go test ./...
```

3. For end-to-end verification, run the containerized harness:

```bash
./scripts/integration-test-containers.sh
```

## Pull requests

- Keep changes focused and include tests when behavior changes.
- Update `README.md` for user-facing flags, defaults, or semantics.
- Ensure CI is green before requesting review.
