package db

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/barnowlsnest/pgqs-harness/postgres"
)

// RunInTx begins a transaction on pool, invokes fn with it, and commits when fn
// returns nil or rolls back otherwise (a panic also triggers a rollback and is
// re-raised). Bind DAOs to the transaction inside fn with BaseDAO.Tx:
//
//	err := db.RunInTx(ctx, pool, func(tx pgx.Tx) error {
//		if _, err := userDAO.Tx(tx).Create(ctx, &u); err != nil {
//			return err
//		}
//		_, err := orderDAO.Tx(tx).Create(ctx, &o)
//		return err
//	})
func RunInTx(ctx context.Context, pool *postgres.DBPool, fn func(tx pgx.Tx) error) error {
	if pool == nil {
		return ErrNilPool
	}

	return pgx.BeginFunc(ctx, pool, fn)
}
