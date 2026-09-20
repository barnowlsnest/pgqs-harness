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

	"github.com/barnowlsnest/pgqs-harness/v2/postgres"
)

var (
	ErrNilPool     = errors.New("pool is nil")
	ErrEmptySchema = errors.New("schema is empty")
	ErrEmptyTable  = errors.New("table is empty")
	ErrNotFound    = errors.New("entity not found")
)

const (
	defaultPingTimeout = time.Second * 5
	idColumn           = "id"
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

// relation returns the schema-qualified table identifier.
func (r *BaseDAO[T]) relation() exp.IdentifierExpression {
	return goqu.S(r.schema).Table(r.table)
}

func (r *BaseDAO[T]) Create(ctx context.Context, entity *T) (*T, error) {
	sql, args, err := postgres.SQL().
		Insert(r.relation()).
		Rows(r.toRecord(entity, idColumn)).
		Returning(goqu.Star()).
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	return r.queryOne(ctx, sql, args)
}

func (r *BaseDAO[T]) GetByID[K comparable](ctx context.Context, id K) (*T, error) {
	return r.GetByCol(ctx, idColumn, id)
}

func (r *BaseDAO[T]) GetByCol[K comparable](ctx context.Context, colName string, val K) (*T, error) {
	sql, args, err := postgres.SQL().
		From(r.relation()).
		Where(goqu.C(colName).Eq(val)).
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	return r.queryOne(ctx, sql, args)
}

func (r *BaseDAO[T]) Update(ctx context.Context, entity *T) (*T, error) {
	return r.UpdateWithColName(ctx, idColumn, entity)
}

func (r *BaseDAO[T]) UpdateWithColName(ctx context.Context, pk string, entity *T) (*T, error) {
	id, ok := r.colValue(entity, pk)
	if !ok {
		return nil, errors.New("entity has no " + pk + " field")
	}

	sql, args, err := postgres.SQL().
		Update(r.relation()).
		Set(r.toRecord(entity, pk)).
		Where(goqu.C(pk).Eq(id)).
		Returning(goqu.Star()).
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	return r.queryOne(ctx, sql, args)
}

func (r *BaseDAO[T]) Delete[K comparable](ctx context.Context, id K) error {
	return r.DeleteByCol(ctx, idColumn, id)
}

func (r *BaseDAO[T]) DeleteByCol[K comparable](ctx context.Context, colName string, val K) error {
	sql, args, err := postgres.SQL().
		Delete(r.relation()).
		Where(goqu.C(colName).Eq(val)).
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

// toRecord builds a goqu.Record from the `db`-tagged fields of entity, omitting
// keyColumn so the database assigns/preserves the key.
func (r *BaseDAO[T]) toRecord(entity *T, keyColumn string) goqu.Record {
	v := reflect.ValueOf(entity).Elem()
	t := v.Type()

	record := make(goqu.Record, t.NumField())
	for i := range t.NumField() {
		column := t.Field(i).Tag.Get("db")
		if column == "" || column == "-" || column == keyColumn {
			continue
		}
		record[column] = columnValue(v.Field(i))
	}

	return record
}

// columnValue prepares a struct field for goqu. goqu expands slice values into
// SQL expression lists and recognizes only the unnamed []byte as a scalar, so a
// named byte slice such as json.RawMessage is converted before it is handed
// over; otherwise it renders as ($1, $2, ...) per byte, or as an empty () when
// nil.
func columnValue(field reflect.Value) any {
	if field.Kind() == reflect.Slice && field.Type().Elem().Kind() == reflect.Uint8 {
		return field.Bytes()
	}

	return field.Interface()
}

// colValue reads the value of the field tagged `db:"<column>"` from entity.
func (r *BaseDAO[T]) colValue(entity *T, column string) (any, bool) {
	v := reflect.ValueOf(entity).Elem()
	t := v.Type()

	for i := range t.NumField() {
		if t.Field(i).Tag.Get("db") == column {
			return v.Field(i).Interface(), true
		}
	}

	return nil, false
}
