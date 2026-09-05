package db

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/barnowlsnest/pgqs-harness/postgres"
)

var (
	ErrNilPool     = errors.New("pool is nil")
	ErrEmptySchema = errors.New("schema is empty")
	ErrEmptyTable  = errors.New("table is empty")
	ErrNotFound    = errors.New("entity not found")
)

const (
	defaultPingTimeout = time.Second * 5
	defaultIDColumn    = "id"
)

type (
	// Querier is the subset of pgx methods BaseDAO needs to issue statements.
	// Both *pgxpool.Pool and pgx.Tx satisfy it, so a DAO can run against the
	// pool directly or be bound to a transaction via Tx.
	Querier interface {
		Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
		Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
		QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	}

	BaseDAO[T any] struct {
		schema      string
		table       string
		idColumn    string
		pingTimeout time.Duration
		pool        *postgres.DBPool
		q           Querier
	}

	// CriteriaFunc yields a single goqu WHERE expression. Find ANDs every
	// provided CriteriaFunc together.
	CriteriaFunc func() exp.Expression
)

func NewBaseDAO[T any](schema, table string, pool *postgres.DBPool) *BaseDAO[T] {
	return &BaseDAO[T]{
		schema:      schema,
		table:       table,
		idColumn:    defaultIDColumn,
		pool:        pool,
		q:           pool,
		pingTimeout: defaultPingTimeout,
	}
}

// Tx returns a shallow copy of the DAO whose statements run on tx instead of
// the pool. The pool reference is retained so Pool, Release and Validate keep
// working; the caller owns the transaction lifecycle (see RunInTx).
func (r *BaseDAO[T]) Tx(tx pgx.Tx) *BaseDAO[T] {
	clone := *r
	clone.q = tx
	return &clone
}

func (r *BaseDAO[T]) WithPingTimeout(timeout time.Duration) *BaseDAO[T] {
	r.pingTimeout = timeout
	return r
}

// WithIDColumn overrides the primary-key column name used by GetByID, Update and Delete.
func (r *BaseDAO[T]) WithIDColumn(name string) *BaseDAO[T] {
	r.idColumn = name
	return r
}

// relation returns the schema-qualified table identifier.
func (r *BaseDAO[T]) relation() exp.IdentifierExpression {
	return goqu.S(r.schema).Table(r.table)
}

func (r *BaseDAO[T]) Create(ctx context.Context, entity *T) (*T, error) {
	sql, args, err := postgres.SQL().
		Insert(r.relation()).
		Rows(r.toRecord(entity)).
		Returning(goqu.Star()).
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	return r.queryOne(ctx, sql, args)
}

func (r *BaseDAO[T]) GetByID(ctx context.Context, id uint64) (*T, error) {
	sql, args, err := postgres.SQL().
		From(r.relation()).
		Where(goqu.C(r.idColumn).Eq(id)).
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	return r.queryOne(ctx, sql, args)
}

func (r *BaseDAO[T]) Update(ctx context.Context, entity *T) (*T, error) {
	id, ok := r.idValue(entity)
	if !ok {
		return nil, errors.New("entity has no " + r.idColumn + " field")
	}

	sql, args, err := postgres.SQL().
		Update(r.relation()).
		Set(r.toRecord(entity)).
		Where(goqu.C(r.idColumn).Eq(id)).
		Returning(goqu.Star()).
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	return r.queryOne(ctx, sql, args)
}

func (r *BaseDAO[T]) Delete(ctx context.Context, id uint64) error {
	sql, args, err := postgres.SQL().
		Delete(r.relation()).
		Where(goqu.C(r.idColumn).Eq(id)).
		Prepared(true).
		ToSQL()
	if err != nil {
		return err
	}

	tag, err := r.q.Exec(ctx, sql, args...)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

func (r *BaseDAO[T]) GetN(ctx context.Context, limit uint64) ([]*T, error) {
	sql, args, err := postgres.SQL().
		From(r.relation()).
		Limit(uint(limit)).
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	return r.queryMany(ctx, sql, args)
}

func (r *BaseDAO[T]) GetAll(ctx context.Context) ([]*T, error) {
	sql, args, err := postgres.SQL().
		From(r.relation()).
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	return r.queryMany(ctx, sql, args)
}

func (r *BaseDAO[T]) Find(ctx context.Context, params ...CriteriaFunc) ([]*T, error) {
	exprs := make([]exp.Expression, 0, len(params))
	for _, param := range params {
		if param == nil {
			continue
		}
		exprs = append(exprs, param())
	}

	sql, args, err := postgres.SQL().
		From(r.relation()).
		Where(exprs...).
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	return r.queryMany(ctx, sql, args)
}

func (r *BaseDAO[T]) Schema() string {
	return r.schema
}

func (r *BaseDAO[T]) Pool() *postgres.DBPool {
	return r.pool
}

func (r *BaseDAO[T]) Release() {
	r.pool.Close()
}

func (r *BaseDAO[T]) Validate() error {
	switch {
	case r.schema == "":
		return ErrEmptySchema
	case r.table == "":
		return ErrEmptyTable
	case r.pool == nil:
		return ErrNilPool
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.pingTimeout)
	defer cancel()

	if err := r.pool.Ping(ctx); err != nil {
		return err
	}

	return nil
}

func (r *BaseDAO[T]) queryOne(ctx context.Context, sql string, args []any) (*T, error) {
	rows, err := r.q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	entity, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByNameLax[T])
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return entity, nil
}

func (r *BaseDAO[T]) queryMany(ctx context.Context, sql string, args []any) ([]*T, error) {
	rows, err := r.q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[T])
}

// toRecord builds a goqu.Record from the `db`-tagged fields of entity,
// omitting the primary-key column so the database assigns/preserves it.
func (r *BaseDAO[T]) toRecord(entity *T) goqu.Record {
	v := reflect.ValueOf(entity).Elem()
	t := v.Type()

	record := make(goqu.Record, t.NumField())
	for i := range t.NumField() {
		column := t.Field(i).Tag.Get("db")
		if column == "" || column == "-" || column == r.idColumn {
			continue
		}
		record[column] = v.Field(i).Interface()
	}

	return record
}

// idValue reads the primary-key field value from entity.
func (r *BaseDAO[T]) idValue(entity *T) (any, bool) {
	v := reflect.ValueOf(entity).Elem()
	t := v.Type()

	for i := range t.NumField() {
		if t.Field(i).Tag.Get("db") == r.idColumn {
			return v.Field(i).Interface(), true
		}
	}

	return nil, false
}
