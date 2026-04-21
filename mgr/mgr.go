package mgr

import (
	"context"
	"errors"
	"net/url"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source"
)

var ErrConfig = errors.New("err nil migration config")

type Config struct {
	DBURL        string
	TargetSchema string
	EmbeddedSRC  source.Driver
}

// Up migrates the tenant schema up.
func Up(ctx context.Context, cfg *Config) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err := cfg.validate(); err != nil {
		return err
	}

	m, err := newMigration(cfg.DBURL, cfg.TargetSchema, cfg.EmbeddedSRC)
	if err != nil {
		return err
	}

	defer func() { _, _ = m.Close() }()

	return m.Up()
}

// Down migrates the tenant schema down.
func Down(ctx context.Context, cfg *Config) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err := cfg.validate(); err != nil {
		return err
	}

	m, err := newMigration(cfg.DBURL, cfg.TargetSchema, cfg.EmbeddedSRC)
	if err != nil {
		return err
	}

	defer func() { _, _ = m.Close() }()

	return m.Down()
}

func newMigration(dbURL, targetSchema string, embeddedSRC source.Driver) (*migrate.Migrate, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("search_path", targetSchema)
	u.RawQuery = q.Encode()

	return migrate.NewWithSourceInstance("iofs", embeddedSRC, u.String())
}

func (cfg *Config) validate() error {
	if cfg == nil {
		return ErrConfig
	}

	if cfg.DBURL == "" {
		return errors.New("db url is required")
	}

	if cfg.TargetSchema == "" {
		return errors.New("target schema is required")
	}

	if cfg.EmbeddedSRC == nil {
		return errors.New("embedded source driver is required")
	}

	return nil
}
