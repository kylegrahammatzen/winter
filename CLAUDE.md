# Winter

Distributed task queue in Go backed by Redis. Go 1.24 minimum.

## Commenting

Exported functions start the comment with the symbol name per Go convention. One sentence for simple things, two or three when there is genuine design reasoning.

Unexported vars and functions use lowercase to start.

Package comments go at the top of the primary file. Multi-sentence is fine.

Inside function and test bodies, only comment what is genuinely non-obvious. Put comments on their own line before the code, never inline at end of a line.

No separators like `// ====` or `// ---`. No dashes in comment text. No bullet lists in comments.

## Commits

Conventional commits following `type(scope): message`. No bullets, no lists, no emojis, no descriptions. Group similar files into commits.

## Rules

- Redis-only, no pluggable storage backends.
- README should only contain features that actually work.
- No `.gitkeep` files.
- Generated proto files (`*.pb.go`) are auto-generated and should never be manually edited or commented.
- `PLAN.md` and `COMMENTING.md` exist locally and are never committed to git.
- Worker return sentinels: `winter.Reschedule(duration)`, `winter.Cancel(reason)`, `winter.SkipRetry`.
- `winter.ClientFromContext(ctx)` allows workers to enqueue downstream jobs.
- CLI lives at `cmd/winter`.
- Canonical log lines: every job emits one structured log line with all context.
- `wintertest` package provides exported test helpers for users.

## Testing

Windows Application Control blocks executables from `%TEMP%`. Root package tests and wintertest must be compiled with `go test -c -o bin/<name>_test.exe` and run directly. Internal package tests work fine with `go test`.
