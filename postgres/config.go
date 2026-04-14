package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/barnowlsnest/go-configlib/v2/pkg/configs"
	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	log "github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
)

const defaultPostgresDriver = "postgresql"

// ErrConfig is the error returned when the configuration is invalid.
var ErrConfig = errors.New("invalid configuration")

// Config represents the configuration for the Postgres database.
type Config struct {
	User string `name:"user" flag:"-"`
	//nolint:gosec // G117: credential loaded from env via configs.Resolve; never logged verbatim (MapToLogFields).
	Pass     string `name:"pass" flag:"-"`
	Database string `name:"database" flag:"-"`
	Host     string `name:"host" default:"127.0.0.1" flag:"-"`
	SSLMode  string `name:"ssl_mode" default:"disable" flag:"-"`
	Port     int    `name:"port" default:"5432" flag:"-"`
}

// ResolveConfig resolves the configuration for the Postgres database.
// Expecting only env variables with prefix "PG_".
// Returns the configuration and an error if the configuration is invalid.
func ResolveConfig() (*Config, error) {
	var pgConfig Config
	_, err := configs.Resolve(&pgConfig, "pg")
	if err != nil {
		return nil, err
	}

	if err := pgConfig.validate(); err != nil {
		return nil, err
	}

	return &pgConfig, nil
}

// DBUrl returns the database URL for the Postgres database.
func (cfg *Config) DBUrl(driverOpt ...string) string {
	var driver string
	switch {
	case len(driverOpt) == 1:
		driver = driverOpt[0]
	default:
		driver = defaultPostgresDriver
	}

	return fmt.Sprintf(
		"%s://%s:%s@%s:%d/%s?sslmode=%s",
		driver, cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode,
	)
}

// MapToLogFields maps the configuration to a slice of logger fields.
func (cfg *Config) MapToLogFields() []logger.Field {
	return []logger.Field{
		log.F("host", cfg.Host),
		log.F("port", cfg.Port),
		log.F("database", cfg.Database),
		log.F("user", cfg.User),
		log.F("pass", strings.Repeat("*", len(cfg.Pass))),
		log.F("ssl_mode", cfg.SSLMode),
	}
}

func (cfg *Config) validate() error {
	switch {
	case cfg == nil:
		return ErrConfig
	case cfg.User == "":
		log.Error(errors.New("missing postgres user"))
		return ErrConfig
	case cfg.Pass == "":
		log.Error(errors.New("missing postgres pass"))
		return ErrConfig
	case cfg.Database == "":
		log.Error(errors.New("missing postgres database"))
		return ErrConfig
	case cfg.Host == "":
		log.Error(errors.New("missing postgres host"))
		return ErrConfig
	case cfg.Port == 0:
		log.Error(errors.New("missing postgres port"))
		return ErrConfig
	case cfg.SSLMode == "":
		log.Error(errors.New("missing postgres ssl mode"))
		return ErrConfig
	}

	return nil
}
