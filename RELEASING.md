# Releasing `raindrop-go`

## Module path

The Go module path is:

```text
github.com/invisible-tools/go-raindrop
```

This repository uses normal Go module tagging.

## Release Steps

1. Update [`version.go`](./version.go) to the release version.
2. Run:

```bash
go vet ./...
go test ./...
go test -race ./...
```

3. Create and push the tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

4. The release workflow will validate the tag and create a GitHub release.
