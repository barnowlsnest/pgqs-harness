//go:build integration

package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/barnowlsnest/pgqs-harness/db"
	"github.com/barnowlsnest/pgqs-harness/postgres"
)

const createWidgets = `
CREATE TABLE IF NOT EXISTS widgets (
	id   BIGSERIAL PRIMARY KEY,
	name TEXT NOT NULL,
	qty  INTEGER NOT NULL DEFAULT 0
);`

type widget struct {
	ID   uint64 `db:"id"`
	Name string `db:"name"`
	Qty  int    `db:"qty"`
}

type BaseDAOSuite struct {
	suite.Suite
	ctx       context.Context
	container *tcpostgres.PostgresContainer
	pool      *postgres.DBPool
	dao       *db.BaseDAO[widget]
}

func TestBaseDAOSuite(t *testing.T) {
	suite.Run(t, new(BaseDAOSuite))
}

func (s *BaseDAOSuite) SetupSuite() {
	if testing.Short() {
		s.T().Skip("docker-backed DAO tests (omit -short to run)")
	}

	s.ctx = context.Background()

	ctr, err := tcpostgres.Run(s.ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("dao_test"),
		tcpostgres.WithUsername("dao"),
		tcpostgres.WithPassword("dao"),
		tcpostgres.BasicWaitStrategies(),
	)
	s.Require().NoError(err)
	s.container = ctr

	connStr, err := ctr.ConnectionString(s.ctx, "sslmode=disable")
	s.Require().NoError(err)

	pool, err := postgres.NewPool(s.ctx, connStr)
	s.Require().NoError(err)
	s.pool = pool

	_, err = pool.Exec(s.ctx, createWidgets)
	s.Require().NoError(err)
}

func (s *BaseDAOSuite) TearDownSuite() {
	if s.container != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s.Require().NoError(s.container.Terminate(shutdownCtx))
	}
}

func (s *BaseDAOSuite) SetupTest() {
	_, err := s.pool.Exec(s.ctx, "TRUNCATE widgets RESTART IDENTITY")
	s.Require().NoError(err)
	s.dao = db.NewBaseDAO[widget]("public", "widgets", s.pool)
}

func (s *BaseDAOSuite) seed(widgets ...widget) []*widget {
	out := make([]*widget, 0, len(widgets))
	for i := range widgets {
		w := widgets[i]
		created, err := s.dao.Create(s.ctx, &w)
		s.Require().NoError(err)
		out = append(out, created)
	}
	return out
}

func (s *BaseDAOSuite) TestValidate_emptyTable() {
	dao := db.NewBaseDAO[widget]("public", "", s.pool)
	s.Require().ErrorIs(dao.Validate(), db.ErrEmptyTable)
}

func (s *BaseDAOSuite) TestValidate_ok() {
	s.Require().NoError(s.dao.Validate())
}

func (s *BaseDAOSuite) TestCreate_assignsGeneratedID() {
	created, err := s.dao.Create(s.ctx, &widget{Name: "gear", Qty: 3})
	s.Require().NoError(err)
	s.Positive(created.ID)
	s.Equal("gear", created.Name)
	s.Equal(3, created.Qty)
}

func (s *BaseDAOSuite) TestGetByID_found() {
	seeded := s.seed(widget{Name: "gear", Qty: 3})

	got, err := s.dao.GetByID(s.ctx, seeded[0].ID)
	s.Require().NoError(err)
	s.Equal(seeded[0].ID, got.ID)
	s.Equal("gear", got.Name)
}

func (s *BaseDAOSuite) TestGetByID_notFound() {
	_, err := s.dao.GetByID(s.ctx, 404)
	s.Require().ErrorIs(err, db.ErrNotFound)
}

func (s *BaseDAOSuite) TestUpdate_persistsChanges() {
	seeded := s.seed(widget{Name: "gear", Qty: 3})
	seeded[0].Name = "cog"
	seeded[0].Qty = 9

	updated, err := s.dao.Update(s.ctx, seeded[0])
	s.Require().NoError(err)
	s.Equal("cog", updated.Name)
	s.Equal(9, updated.Qty)

	reloaded, err := s.dao.GetByID(s.ctx, seeded[0].ID)
	s.Require().NoError(err)
	s.Equal("cog", reloaded.Name)
	s.Equal(9, reloaded.Qty)
}

func (s *BaseDAOSuite) TestDelete_removesRow() {
	seeded := s.seed(widget{Name: "gear"})

	s.Require().NoError(s.dao.Delete(s.ctx, seeded[0].ID))

	_, err := s.dao.GetByID(s.ctx, seeded[0].ID)
	s.Require().ErrorIs(err, db.ErrNotFound)
}

func (s *BaseDAOSuite) TestDelete_notFound() {
	s.Require().ErrorIs(s.dao.Delete(s.ctx, 404), db.ErrNotFound)
}

func (s *BaseDAOSuite) TestGetN_limitsResults() {
	s.seed(widget{Name: "a"}, widget{Name: "b"}, widget{Name: "c"})

	got, err := s.dao.GetN(s.ctx, 2)
	s.Require().NoError(err)
	s.Len(got, 2)
}

func (s *BaseDAOSuite) TestGetAll_returnsEveryRow() {
	s.seed(widget{Name: "a"}, widget{Name: "b"}, widget{Name: "c"})

	got, err := s.dao.GetAll(s.ctx)
	s.Require().NoError(err)
	s.Len(got, 3)
}

func (s *BaseDAOSuite) TestFind_appliesCriteria() {
	s.seed(widget{Name: "keeper", Qty: 1}, widget{Name: "other", Qty: 1}, widget{Name: "keeper", Qty: 2})

	got, err := s.dao.Find(s.ctx,
		func() exp.Expression { return goqu.C("name").Eq("keeper") },
		func() exp.Expression { return goqu.C("qty").Gt(1) },
	)
	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("keeper", got[0].Name)
	s.Equal(2, got[0].Qty)
}
