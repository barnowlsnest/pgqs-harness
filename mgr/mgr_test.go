//go:build integration

package mgr_test

import (
	"context"
	"embed"
	"fmt"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	_ "github.com/barnowlsnest/pgqs-harness/mgr"
)

//go:embed testdata/migrations/*.sql
var migrationsFS embed.FS

type MgrSuite struct {
	suite.Suite
	ctx       context.Context
	container *tcpostgres.PostgresContainer
	connStr   string
	pool      *pgxpool.Pool
	schemaSeq int
}

func TestMgrSuite(t *testing.T) {
	suite.Run(t, new(MgrSuite))
}

func (s *MgrSuite) SetupSuite() {
	if testing.Short() {
		s.T().Skip("docker-backed mgr tests (omit -short to run)")
	}

	s.ctx = context.Background()

	ctr, err := tcpostgres.Run(s.ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("mgr_test"),
		tcpostgres.WithUsername("mgr"),
		tcpostgres.WithPassword("mgr"),
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

func (s *MgrSuite) TearDownSuite() {
	if s.pool != nil {
		s.pool.Close()
	}
	if s.container != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s.Require().NoError(s.container.Terminate(shutdownCtx))
	}
}

// newSourceDriver creates a fresh iofs source driver from the embedded migrations.
// Each call returns an independent driver (golang-migrate closes the driver on m.Close()).
func (s *MgrSuite) newSourceDriver() source.Driver {
	d, err := iofs.New(migrationsFS, "testdata/migrations")
	s.Require().NoError(err)
	return d
}

// createSchema creates a unique Postgres schema for test isolation and returns its name.
func (s *MgrSuite) createSchema() string {
	s.schemaSeq++
	schema := fmt.Sprintf("test_mgr_%d", s.schemaSeq)
	_, err := s.pool.Exec(s.ctx, fmt.Sprintf("CREATE SCHEMA %s", schema))
	s.Require().NoError(err)
	return schema
}

// tableExists checks whether a table exists in the given schema.
func (s *MgrSuite) tableExists(schema, table string) bool {
	var exists bool
	err := s.pool.QueryRow(s.ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
		schema, table,
	).Scan(&exists)
	s.Require().NoError(err)
	return exists
}
