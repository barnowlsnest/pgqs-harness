//go:build integration

package db_test

import (
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/barnowlsnest/pgqs-harness/db"
)

func (s *BaseDAOSuite) TestRunInTx_commitsOnSuccess() {
	err := db.RunInTx(s.ctx, s.pool, func(tx pgx.Tx) error {
		_, err := s.dao.Tx(tx).Create(s.ctx, &widget{Name: "gear", Qty: 1})
		return err
	})
	s.Require().NoError(err)

	got, err := s.dao.GetAll(s.ctx)
	s.Require().NoError(err)
	s.Len(got, 1)
}

func (s *BaseDAOSuite) TestRunInTx_rollsBackOnError() {
	sentinel := errors.New("boom")

	err := db.RunInTx(s.ctx, s.pool, func(tx pgx.Tx) error {
		txDAO := s.dao.Tx(tx)
		if _, err := txDAO.Create(s.ctx, &widget{Name: "gear", Qty: 1}); err != nil {
			return err
		}
		return sentinel
	})
	s.Require().ErrorIs(err, sentinel)

	got, err := s.dao.GetAll(s.ctx)
	s.Require().NoError(err)
	s.Empty(got)
}

func (s *BaseDAOSuite) TestTx_isolatesUncommittedWrites() {
	err := db.RunInTx(s.ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := s.dao.Tx(tx).Create(s.ctx, &widget{Name: "gear"}); err != nil {
			return err
		}

		// The pool-bound DAO cannot see the row until the tx commits.
		outside, err := s.dao.GetAll(s.ctx)
		s.Require().NoError(err)
		s.Empty(outside)

		// The tx-bound DAO can.
		inside, err := s.dao.Tx(tx).GetAll(s.ctx)
		s.Require().NoError(err)
		s.Len(inside, 1)

		return nil
	})
	s.Require().NoError(err)
}

func (s *BaseDAOSuite) TestRunInTx_nilPool() {
	s.Require().ErrorIs(db.RunInTx(s.ctx, nil, func(pgx.Tx) error { return nil }), db.ErrNilPool)
}
