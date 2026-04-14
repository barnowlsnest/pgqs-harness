package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/task"
	"github.com/jackc/pgx/v5/pgxpool"

	log "github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
)

const (
	maxPingAttempts = 5
	pingInterval    = time.Second
)

type DBPool = pgxpool.Pool

// NewPool initializes and returns a new database connection pool using the provided context and configuration.
// It validates the context and configuration, establishes a connection pool, and performs initial health checks.
// If any step fails, an error is returned.
func NewPool(ctx context.Context, dbURL string) (*DBPool, error) {
	switch {
	case ctx == nil:
		return nil, fmt.Errorf("invalid ctx: %w", errors.New("context is nil"))
	case dbURL == "":
		return nil, fmt.Errorf("invalid db url: %w", errors.New("db url is empty"))
	}

	pgPool, errConfig := pgxpool.New(ctx, dbURL)
	if errConfig != nil {
		return nil, errConfig
	}

	b := task.NewBuilder(
		task.WithID(1),
		task.WithName("pg-ping"),
		task.WithDelay(pingInterval),
		task.WithMaxRetries(maxPingAttempts),
		task.WithHooks(taskHooks()),
		task.WithTaskFn(ping(pgPool)),
	)

	def, errTaskDef := b.Build()
	if errTaskDef != nil {
		return nil, errTaskDef
	}

	t := task.New(*def)
	if errRetry := t.GoRetry(ctx); errRetry != nil {
		log.Error(errRetry)
		return nil, errRetry
	}

	return pgPool, nil
}

func ping(pool *pgxpool.Pool) func(r *task.Run) error {
	return func(r *task.Run) error {
		log.Debug("attempt postgres ping")
		return pool.Ping(r.Context())
	}
}
