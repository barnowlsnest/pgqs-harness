//go:build integration

package db_test

import (
	"context"
	"embed"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/barnowlsnest/pgqs-harness/db"
)

//go:embed testdata/migrations/*.sql
var migrationsFS embed.FS

const migrationsDir = "testdata/migrations"

type MigrateSuite struct {
	suite.Suite
	ctx       context.Context
	container *tcpostgres.PostgresContainer
	connStr   string
	pool      *pgxpool.Pool
}

func TestMigrateSuite(t *testing.T) {
	suite.Run(t, new(MigrateSuite))
}

func (s *MigrateSuite) SetupSuite() {
	if testing.Short() {
		s.T().Skip("docker-backed migration tests (omit -short to run)")
	}

	s.ctx = context.Background()

	ctr, err := tcpostgres.Run(s.ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("migrate_test"),
		tcpostgres.WithUsername("migrate"),
		tcpostgres.WithPassword("migrate"),
		tcpostgres.BasicWaitStrategies(),
	)
	s.Require().NoError(err)
	s.container = ctr

	connStr, err := ctr.ConnectionString(s.ctx, "sslmode=disable")
	s.Require().NoError(err)
	s.connStr = connStr

	pool, err := pgxpool.New(s.ctx, connStr)
	s.Require().NoError(err)
	s.pool = pool
	s.Require().NoError(pool.Ping(s.ctx))
}

func (s *MigrateSuite) TearDownSuite() {
	if s.pool != nil {
		s.pool.Close()
	}
	if s.container != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s.Require().NoError(s.container.Terminate(shutdownCtx))
	}
}

func (s *MigrateSuite) tableExists(table string) bool {
	var exists bool
	err := s.pool.QueryRow(s.ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
		table,
	).Scan(&exists)
	s.Require().NoError(err)
	return exists
}

func (s *MigrateSuite) TestRollOutThenRollDown() {
	s.Require().NoError(db.RollOut(s.ctx, &migrationsFS, s.connStr, migrationsDir))
	s.True(s.tableExists("rollout_probe"), "RollOut should create the migration table")

	s.Require().NoError(db.RollDown(s.ctx, &migrationsFS, s.connStr, migrationsDir))
	s.False(s.tableExists("rollout_probe"), "RollDown should drop the migration table")
}

func (s *MigrateSuite) TestRollOut_unknownMigrationsDir() {
	err := db.RollOut(s.ctx, &migrationsFS, s.connStr, "testdata/does-not-exist")
	s.Require().Error(err)
}

func (s *MigrateSuite) TestRollOut_invalidDBURL() {
	err := db.RollOut(s.ctx, &migrationsFS, "://not-a-url", migrationsDir)
	s.Require().Error(err)
}

func (s *MigrateSuite) TestRollDown_canceledContext() {
	ctx, cancel := context.WithCancel(s.ctx)
	cancel()

	err := db.RollDown(ctx, &migrationsFS, s.connStr, migrationsDir)
	s.Require().ErrorIs(err, context.Canceled)
}
