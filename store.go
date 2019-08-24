package migrate

import (
	"database/sql"
)

type Store interface {
	Open() error
	Exec(string, ...interface{}) (sql.Result, error)

	CreateMetaIfNotExists() error
	CreateMetaCheckpointsIfNotExists() error

	GetMigrations() ([]Migration, error)
	InsertMigration(filename, checksum string) error
	UpsertMigration(filename, checksum string) error

	GetMetaCheckpoints(string) ([]string, error)
	InsertMetaCheckpoint(filename, checksum string, idx int) error
	DeleteMetaCheckpoints() error
}
