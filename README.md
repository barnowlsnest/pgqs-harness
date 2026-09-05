# pgqs-harness

Shared PostgreSQL infrastructure library for PGQS services. Provides connection pool management, `LISTEN`/`NOTIFY` support, SQL query building, schema migrations, a generic CRUD DAO, and configuration helpers built on top of [pgx](https://github.com/jackc/pgx).

## Packages

### `postgres`

Core database utilities.

| Symbol                                | Description                                                                  |
|---------------------------------------|------------------------------------------------------------------------------|
| `Config` / `ResolveConfig()`          | Loads connection settings from `PG_*` environment variables                  |
| `NewPool(ctx, dbURL)`                 | Creates a `pgxpool.Pool` with built-in retry ping (5 attempts, 1 s interval) |
| `NewListener` / `NewListenerFromPool` | `LISTEN`/`NOTIFY` wrapper with a buffered notification channel               |
| `SQL()`                               | Returns a singleton `goqu` postgres dialect for type-safe query building     |

### `mgr`

Schema migration runner built on [golang-migrate](https://github.com/golang-migrate/migrate).

| Function         | Description                    |
|------------------|--------------------------------|
| `Up(ctx, cfg)`   | Runs all pending migrations up |
| `Down(ctx, cfg)` | Rolls all migrations back      |

`mgr.Config` fields:

| Field          | Description                                                 |
|----------------|-------------------------------------------------------------|
| `DBURL`        | PostgreSQL connection URL                                   |
| `TargetSchema` | Optional schema name — sets `search_path` on the connection |
| `EmbeddedSRC`  | `source.Driver` pointing at embedded SQL migration files    |

### `db`

Application-facing layer built on `postgres` and `mgr`.

| Symbol                                                    | Description                                                                        |
|----------------------------------------------------------|-----------------------------------------------------------------------------------|
| `RollOut(ctx, embedFS, dbURL, dir)`                       | Builds an `iofs` source from an `embed.FS` and runs migrations up                  |
| `RollDown(ctx, embedFS, dbURL, dir)`                      | Same, rolling all migrations back                                                  |
| `NewBaseDAO[T](schema, table, pool)`                      | Generic CRUD DAO over a schema-qualified table; struct fields mapped via `db:` tags |

`BaseDAO[T]` methods: `Create`, `GetByID`, `Update`, `Delete`, `GetN`, `GetAll`, `Find` (ANDs variadic
`CriteriaFunc` predicates), plus `Validate` (pings the pool). Options: `WithIDColumn` (default `id`, omitted
on writes so the database assigns it), `WithPingTimeout`. Missing rows return `db.ErrNotFound`.

## Configuration

`ResolveConfig()` reads the following environment variables (prefix `PG_`):

| Variable      | Default     | Description                  |
|---------------|-------------|------------------------------|
| `PG_USER`     | —           | Database user (required)     |
| `PG_PASS`     | —           | Database password (required) |
| `PG_DATABASE` | —           | Database name (required)     |
| `PG_HOST`     | `127.0.0.1` | Host                         |
| `PG_PORT`     | `5432`      | Port                         |
| `PG_SSL_MODE` | `disable`   | SSL mode                     |

## Usage

```go
// Connect
cfg, err := postgres.ResolveConfig()
pool, err := postgres.NewPool(ctx, cfg.DBUrl())

// LISTEN/NOTIFY
listener, err := postgres.NewListenerFromPool(ctx, pool, "my-channel", 16)
listener.Start(ctx)
for n := range listener.Notifications() {
    fmt.Println(n.Payload)
}
listener.Stop(5 * time.Second)

// Query building
sql, _, _ := postgres.SQL().From("orders").Where(goqu.C("id").Eq(42)).ToSQL()

// Migrations (embed your SQL files with //go:embed)
err = mgr.Up(ctx, &mgr.Config{
    DBURL:        cfg.DBUrl(),
    TargetSchema: "tenant_xyz",
    EmbeddedSRC:  migrationsDriver,
})

// ...or from an embed.FS directly
//go:embed migrations/*.sql
var migrationsFS embed.FS
err = db.RollOut(ctx, &migrationsFS, cfg.DBUrl(), "migrations")

// Generic CRUD
type Order struct {
    ID     uint64 `db:"id"`
    Status string `db:"status"`
}
dao := db.NewBaseDAO[Order]("public", "orders", pool)
o, err := dao.Create(ctx, &Order{Status: "new"})
open, err := dao.Find(ctx, func() exp.Expression { return goqu.C("status").Eq("new") })
```

## Development

Requires [Task](https://taskfile.dev) and Docker (for integration tests via Testcontainers).

```sh
task build   # compile
task test    # run tests with race detector (requires Docker)
task lint    # vet, fmt, goimports, golangci-lint
task update  # go mod tidy
```

## License

See [LICENSE](LICENSE).
