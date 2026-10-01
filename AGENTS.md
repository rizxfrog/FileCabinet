# Repository Guidelines

Go backend for FileCabinet: a content-addressed (SHA-256) file store that brokers
ModelScope (LFS/dataset repos) with metadata and coordination in Redis, plus a React UI
compiled into the same binary. Module `FileCabinet`, Go 1.24.

## Project Overview

- Key = SHA-256 of file content. Clients compute the hash; the service stores bytes
  under `namespace/<hash[:2]>/<hash[2:]>` in a configured ModelScope repo.
- Each file also gets a committed JSON metadata document at
  `namespace/meta/<hash[:2]>/<hash[2:]>.json` holding `file_name` and `size`. It is
  written in the same commit as the blob pointer, is the authoritative source of the
  name on read, and lets a lookup rebuild a record when the Redis entry is gone
  (recovered records have no expiry).
- Metadata, short codes, expiry index and upload sessions live in Redis; Redis also
  gates single-instance ownership.
- Two modes per direction: `UPLOAD_MODE` and `DOWNLOAD_MODE`, each `proxy` (default,
  bytes stream through this service) or `direct` (browser talks to upstream storage).
- Files expire (1h/1d/7d/custom/never) and are deleted upstream by a background cleanup
  loop. Short codes (6-digit, then 6-char alphanumeric) resolve hash -> file.
- The ModelScope token never leaves the process; only `proxy`-derived headers are
  returned to browsers.

## Architecture & Data Flow

Layered, one-directional: `main` -> `app` -> `store` / `modelscope` -> `config`.

```
config.Load() -> redis client -> store.New(r, c.Prefix()) -> s.ClaimOrReclaim()
  -> app.New(c, s, modelscope.New(c)) -> app.Handler(embedFS) behind app.Lifecycle
```

Upload (proxy): `PUT /api/files/{hash}` -> `Service.Upload` -> per-hash lock ->
`ValidateFile` -> `ready` (owner + pending journal) -> delete stale duplicate -> tee
SHA-256 while reading (`counter` + `LimitReader(size+1)`) -> `Upload.Validate` (batch API)
-> `Client.Put` (stream to storage) or `Commit` (base64 inline when `size <= InlineLimit`)
-> `store.Save`.

Upload (direct): `POST /api/uploads/prepare` -> `Service.Prepare` stores a session ->
client PUTs to the signed upstream URL itself -> `POST /api/uploads/{id}/complete` ->
`Service.Complete` re-runs `Validate` and commits only when the blob is no longer needed.

Download: `Service.Open`/`DirectDownload` -> `authorize` (resolve key/short code, take
lock, verify owner, pending, expiry, code validity) -> `Client.Download` (stream, follows
up to 5 allowlisted redirects) or `Client.DownloadTarget` (returns the signed URL, no
redirect following). Bytes are relayed by `streamFile` only in proxy mode; the per-hash
lock is held until the response body closes (`lockedBody`) and released immediately when
a signed URL is returned.

Cleanup: a 30s ticker calls `Service.Cleanup`, which processes up to 100 due hashes from
the `expires` sorted set, rescheduling uncertain outcomes rather than deleting.

Coordination invariants (README "单实例与故障恢复" covers recovery):

- `owner` key has **no TTL**; a crash blocks restart by design, not by bug.
- `pending` hash journal (`commit` / `delete` / `delete-inflight` / `delete-uncertain`)
  fences in-flight mutations; `ErrBusy` is returned while any entry exists.
- `Service.Uncertain` is sticky; when set, `main` never calls `store.Release`.

## Key Directories

| Path | Purpose |
| --- | --- |
| `main.go` | Entry point: embed, wiring, signal handling, drain, owner release. |
| `internal/config` | Env parsing/validation, Redis key prefix derivation. |
| `internal/store` | Redis persistence: files, expiry zset, codes, sessions, owner/pending. |
| `internal/modelscope` | ModelScope API + storage transport, SSRF guards, credential isolation. |
| `internal/app` | Domain service (`service.go`) and HTTP surface (`http.go`). |
| `public/` | **Generated** built UI (embedded). Frontend source lives in the sibling repo `../FileCabinetFront`. |
| `scripts/build.sh` | Rebuilds the UI from the sibling repo and produces `bin/filecabinet`. |
| `README.md` | Authoritative product/ops/API documentation (Chinese). |
| `goal.md` | Original design intent; stale relative to code — README wins. |

## Development Commands

```sh
# Backend
go run .                 # needs .env with MODELSCOPE_TOKEN and MODELSCOPE_REPO_ID, Redis
go build ./...
go vet ./...

# Tests (from README:201-203)
go test ./... -count=1
go test -race ./...
go test ./internal/app/ -run TestDownloadDirect -v

# UI rebuild (requires Bun + sibling ../FileCabinetFront with bun.lock)
./scripts/build.sh       # wipes public/, copies dist/, builds bin/filecabinet
./bin/filecabinet

# Deployment (build context is the PARENT of both repos)
cp .env.example .env
docker compose up --build -d
```

No Makefile, CI config, or linter config exists in this repo — nothing enforces style
automatically, so run `gofmt -l .` and `go vet ./...` manually before yielding.

## Code Conventions & Common Patterns

- **Layering**: `app` must not touch `*redis.Client` directly — go through `store`.
  Upstream access must go through the `Upstream` interface in `internal/app/service.go`
  so tests can substitute a fake.
- **Errors**: package-level sentinels with Chinese user-facing text —
  `store.ErrMissing`, `store.ErrOwner`, `modelscope.ErrUpstream`,
  `modelscope.ErrDirectUnsupported`, `modelscope.ErrUncertainMutation`,
  `app.ErrInvalid`, `app.ErrBusy`. Wrap with `%w` (full-width `：` for user detail) or
  `errors.Join` (`fmtInvalid`); dispatch with `errors.Is`. HTTP mapping lives only in
  `fail` (`internal/app/http.go`): 400 / 404 / 409 / 422 / 502 / 504 / 503 default.
- **Uncertainty**: transport failures and ambiguous upstream envelopes map to
  `ErrUncertainMutation`, never to a clean error; the journal entry is kept and the
  mutation is never auto-retried.
- **Upstream delete**: the `DELETE .../repo` endpoint now answers 400 ("token no longer
  supports deletion operations"), so deletion commits minimal `{"action":"delete","path":…}`
  actions instead. ModelScope rejects a commit containing a path absent from the
  revision, so existence is probed first and absent paths are skipped; a failed probe is
  `ErrUpstream` (safe to retry, nothing mutated).
- **Context**: first parameter on every I/O method. Where a journal write must survive
  cancellation, use `context.WithoutCancel(ctx)` with a short timeout.
- **Concurrency**: in-process per-hash lock is `[256]chan struct{}` indexed by the first
  byte of `sha256(hash)` — coarse buckets, unrelated hashes may serialize. `lock` returns
  an `unlock func()` that MUST be called exactly once. Cross-process safety comes from
  Redis, not this lock. Download locks are released from `lockedBody.Close`.
- **Logging**: stdlib `log/slog` only, structured key/value, English messages. Never log
  tokens, signed URLs, or session IDs.
- **Language**: identifiers, JSON tags and log lines are English; user-facing error text
  and README are Chinese.
- **DI / seams**: constructor injection (`store.New`, `modelscope.New`, `app.New`);
  `Service.Now` and `Service.Generate` are the injected clock and code generator used by
  tests. No globals beyond `slog`.
- **Redis writes**: multi-key mutations use `TxPipelined`; claim/code allocation uses
  `SetNX`; the only Lua script is the compare-and-delete in `store.Release`.
- **Merge semantics**: `MergeExpiry` treats `0` (never expires) as winning, otherwise max.
  Do not "fix" this to min.

## Important Files

| File | Why it matters |
| --- | --- |
| `main.go` | Startup ordering and shutdown sequence (claim -> serve -> drain -> release). |
| `internal/app/service.go` | `Upstream` interface, per-hash lock, `Upload`/`Prepare`/`Complete`/`authorize`/`Open`/`DirectDownload`/`Cleanup`. |
| `internal/app/http.go` | Route table, `Lifecycle` admission control, CSP/CSRF middleware, 16-slot limiter, `reply`/`fail`/`streamFile`. |
| `internal/modelscope/client.go` | `SafeStorageURL`, `uploadTarget` (exact-URL credential scope), `Put`/`Commit`/`Delete`/`Download`/`DownloadTarget`. |
| `internal/store/store.go` | Key layout, owner claim/reclaim/release, pending journal, expiry zset. |
| `internal/config/config.go` | `Config`, `Load`, `Validate` (startup contract), `Prefix` (Redis namespace). |
| `.env.example` | Every environment variable with semantics and warnings. |

HTTP routes (all registered in `Service.Handler`):
`GET /healthz`, `GET /api/config`, `PUT /api/files/{hash}`, `GET /api/files/{key}`,
`POST /api/codes`, `POST /api/uploads/prepare`, `POST /api/uploads/{id}/complete`,
`GET /api/download/{key}`, JSON 404 under `/api/`, embedded UI on `/`.

## Runtime/Tooling Preferences

- **Go 1.24** (`go.mod`), `CGO_ENABLED=0`, `-trimpath` for release builds.
- **Bun 1.x** for the frontend only (`FileCabinetFront`, not this repo). No npm/yarn.
- **Redis 7** with persistence enabled (`appendonly yes`, `appendfsync always`,
  `maxmemory-policy noeviction`). Lossy Redis breaks the ownership guarantee.
- Dependencies are intentionally minimal: `redis/go-redis/v9`, `joho/godotenv`,
  `alicebob/miniredis/v2` (test-only, direct require is expected). Do not add a mock or
  assertion library.
- **One active instance per storage identity.** Never scale replicas, never delete the
  Redis volume to clear a blocked claim, never shorten `stop_grace_period` below
  `TRANSFER_TIMEOUT`.
- `public/` is committed generated output: do not hand-edit it, do not gitignore it
  (`//go:embed public` and `main_test.go` both need it). Regenerate via `scripts/build.sh`.
- Never widen `STORAGE_HOST_SUFFIXES` (`.com`-style suffixes defeat the SSRF allowlist)
  and never expose `MODELSCOPE_TOKEN`, session IDs, or signed URLs.
- Docker builds only via compose from this directory (`context: ..`); a plain
  `docker build .` here fails because the Dockerfile also copies the sibling repo.

## Testing & QA

- **Framework**: standard library `testing` only. Assertions are guard-clause
  `t.Fatal`/`t.Fatalf` with `errors.Is` on sentinels. No testify, no build tags, no
  `t.Parallel`, no coverage thresholds, no env vars in tests.
- **Fakes**: Redis is always `miniredis.RunT(t)` (use `mr.FastForward`, `mr.SetError`);
  upstream is either `fakeUpstream` (implements `app.Upstream`, with failure flags and
  call counters) or a real `modelscope.Client` whose `API`/`Storage` transports are
  swapped for function `RoundTripper`s. HTTP is exercised with `httptest.NewServer`
  (lifecycle/drain) and `httptest.NewRecorder` (isolated handlers); the UI is embedded in
  `main_test.go` against the real `public/`.
- **Time**: never sleep. Inject `Service.Now` (frozen at a fixed Unix second in `setup`)
  and use `miniredis.FastForward` for Redis TTLs. `afterSaveHook`/`afterCommit` hooks
  make expiry cross exactly after a store commit.
- **Wire format**: ModelScope responses are hand-written JSON captured from the real API
  (`lfsFixture` in `internal/modelscope/client_test.go`) — treat them as records, not
  invented shapes. Security tests assert exact behaviour: token absence, exact-URL
  credential scope, redirect rejection, envelope classification.
- **Layout**: tests are white-box, same-package (`package app`, `package modelscope`, …).
  They reach unexported fields (`a.lock`, `a.Uncertain`, `a.S.R`, store `Prefix`/`Owner`),
  and rename internals will break them — expected.
- **Coverage expectations**: regression tests accompany every behaviour change (see the
  "验证" section of README for the current inventory). When you change upload/download
  semantics, extend `internal/app/service_test.go` and
  `internal/modelscope/client_test.go`; when you change env rules, extend
  `internal/config/config_test.go`.
- **Before finishing**: run `go build ./...`, `go test ./... -count=1` and
  `go test -race ./...`. Frontend behaviour (worker hashing, UI flow, e2e) is tested in
  the sibling repo with `bun test` / `bun run test:e2e` and is out of scope here.
