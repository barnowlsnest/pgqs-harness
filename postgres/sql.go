package postgres

import (
	"sync"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
)

var (
	once    sync.Once
	dialect goqu.DialectWrapper
)

type Dialect = goqu.DialectWrapper

// SQL returns the SQL dialect for the Postgres database.
func SQL() Dialect {
	once.Do(func() {
		dialect = goqu.Dialect("postgres")
	})

	return dialect
}
