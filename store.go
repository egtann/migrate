package migrate

import (
	"database/sql"
)

type Store interface {
	Open() error
	Exec(string, ...interface{}) (sql.Result, error)

	// CreateMetaversionIfNotExists and report the current version.
	CreateMetaVersionIfNotExists() (int, error)
	CreateMetaIfNotExists() error
	CreateMetaCheckpointsIfNotExists() error

	GetMigrations() ([]Migration, error)
	InsertMigration(filename, content, checksum string) error
	UpsertMigration(filename, content, checksum string) error

	GetMetaCheckpoints(string) ([]string, error)
	InsertMetaCheckpoint(filename, content, checksum string, idx int) error
	DeleteMetaCheckpoints() error

	UpdateMetaVersion(int) error

	UpgradeToV1([]Migration) error
}
