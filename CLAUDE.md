# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`pgqs-harness` is a **library** (no `main`) providing shared PostgreSQL infrastructure for PGQS services:
connection-pool management with retry ping, `LISTEN`/`NOTIFY`, `goqu` query building, schema migrations, a
generic CRUD DAO, and env-based config. Built on `pgx/v5`.

## Commands

Uses [Task](https://taskfile.dev). Docker is required for tests (Testcontainers spins up real Postgres).

```sh
task build   # go build ./...
task test    # go clean -testcache && go test -v -race -timeout 5m -tags integration ./...
task lint    # go vet, go fmt, goimports -w, golangci-lint run --fix
task update  # go mod tidy (sets GOPRIVATE=github.com/barnowlsnest/*)
```

Run a single test:

```sh
go test -race -tags integration -run TestBaseDAOSuite/TestGetAll_returnsEveryRow ./db/...
```

- `db/` has two `testify/suite` entrypoints: `TestBaseDAOSuite` and `TestMigrateSuite`. Subtests are the
  suite method names (e.g. `TestGetN_limitsResults`), so `-run TestBaseDAOSuite/<Method>` targets one case.

- Integration tests are gated by the `integration` build tag and the `//go:build integration` constraint;
  without the tag most test files compile to nothing.
- CI runs `task build` + `task test` (build.yml) and `golangci-lint` v2 (lint.yml) on PRs to `main`.
- Dependencies on `github.com/barnowlsnest/*` are private modules — `GOPRIVATE` must be set for `go mod tidy`.

## Architecture

Three packages, layered bottom-up:

### `postgres` — connection primitives
- `config.go`: `Config` + `ResolveConfig()` read `PG_*` env vars via `go-configlib`. `DBUrl()` builds the
  connection string. `MapToLogFields()` masks the password — never log `Config` directly.
- `pool.go`: `NewPool` wraps `pgxpool.New` and drives an initial health check through `go-asynctasklib`
  (`task.go` supplies the retry hooks): 5 attempts, 1s apart. `DBPool` is a type alias for `pgxpool.Pool`.
- `listen.go`: `Listener` wraps one acquired `*pgxpool.Conn` for `LISTEN`/`NOTIFY`. `Start` is `sync.Once`,
  spawns a goroutine feeding a buffered `Notifications()` channel; `Stop` cancels, waits, `UNLISTEN`s, and
  releases the conn. The listener holds a dedicated connection for its whole lifetime. Errors from the
  listen loop are not returned — they surface asynchronously via `Err()` (set internally by `setErr`).
- `sql.go`: `SQL()` returns a singleton `goqu` postgres dialect for query building.

### `mgr` — migration runner
Thin wrapper over `golang-migrate/v4`. `Up`/`Down` take a `Config{DBURL, TargetSchema, EmbeddedSRC}`.
`EmbeddedSRC` is a `source.Driver` (typically from `iofs`). `TargetSchema`, when set, is injected as
`search_path` in the connection URL so migrations target a specific (tenant) schema.

### `db` — application-facing layer
- `db.go`: `RollOut`/`RollDown` — convenience wrappers that build an `iofs` driver from an `embed.FS` and
  call `mgr.Up`/`mgr.Down`.
- `basedao.go`: `BaseDAO[T]` — generic CRUD over a schema-qualified table using a `*postgres.DBPool`.
  Struct fields are mapped via `db:"..."` tags. Reflection (`toRecord`, `idValue`) builds `goqu.Record`s and
  **omits the id column** on write so the DB assigns it; `pgx.RowTo*StructByNameLax` scans results.
  Not-found is normalized to `ErrNotFound`. `idColumn` defaults to `"id"` (override with `WithIDColumn`;
  `WithPingTimeout` sets the `Validate` ping deadline — both are chainable on the DAO).
  Read methods: `GetByID`, `GetN(limit)`, `GetAll`, and `Find`, which ANDs together variadic
  `CriteriaFunc` closures that each yield a `goqu` expression.
  Statements run through a `Querier` interface (satisfied by both `*pgxpool.Pool` and `pgx.Tx`);
  `dao.Tx(tx)` returns a shallow copy bound to a transaction while keeping the pool ref for
  `Pool`/`Release`/`Validate`.
- `tx.go`: `RunInTx(ctx, pool, func(tx pgx.Tx) error)` — wraps `pgx.BeginFunc` (commit on nil, rollback
  otherwise). Bind DAOs inside the closure with `dao.Tx(tx)` to run several across one transaction.

## Conventions

- `goimports` local-prefix is `github.com/barnowlsnest/pgqs-harness`.
- Logging goes through `go-logslib` (`log "github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"`).
- golangci-lint is strict (`gosec`, `gocritic` incl. opinionated/performance, `funlen` 100 lines, `gocyclo` 15,
  `lll` 140); `_test.go` files are exempted from several of these.
