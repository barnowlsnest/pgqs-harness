package db

import (
	"context"
	"embed"

	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/barnowlsnest/pgqs-harness/mgr"
)

// RollOut applies pgqs database migrations.
func RollOut(ctx context.Context, embeddedMigrations *embed.FS, dbURL, dirMigrations string) error {
	driver, err := iofs.New(embeddedMigrations, dirMigrations)
	if err != nil {
		return err
	}

	return mgr.Up(ctx, &mgr.Config{
		DBURL:       dbURL,
		EmbeddedSRC: driver,
	})
}

// RollDown rolls down pgqs database migrations.
func RollDown(ctx context.Context, embeddedMigrations *embed.FS, dbURL, dirMigrations string) error {
	driver, err := iofs.New(embeddedMigrations, dirMigrations)
	if err != nil {
		return err
	}

	return mgr.Down(ctx, &mgr.Config{
		DBURL:       dbURL,
		EmbeddedSRC: driver,
	})
}
