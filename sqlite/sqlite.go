package sqlite

import (
	"github.com/egtann/migrate"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	filepath string

	// Embed the sqlx DB struct
	*sqlx.DB
}

func New(dbFile string) *DB {
	return &DB{filepath: dbFile}
}

func (db *DB) CreateMetaIfNotExists() error {
	q := `CREATE TABLE IF NOT EXISTS meta (
		filename TEXT UNIQUE NOT NULL,
		md5 TEXT NOT NULL,
		createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`
	if _, err := db.Exec(q); err != nil {
		return errors.Wrap(err, "create meta table")
	}
	return nil
}

func (db *DB) CreateMetaCheckpointsIfNotExists() error {
	q := `CREATE TABLE IF NOT EXISTS metacheckpoints (
		filename TEXT NOT NULL,
		idx INTEGER NOT NULL,
		md5 TEXT NOT NULL,
		createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (filename, idx)
	)`
	if _, err := db.Exec(q); err != nil {
		return errors.Wrap(err, "create metacheckpoints table")
	}
	return nil
}

func (db *DB) GetMigrations() ([]migrate.Migration, error) {
	migrations := []migrate.Migration{}
	q := `SELECT filename, md5 AS checksum FROM meta`
	err := db.Select(&migrations, q)
	return migrations, err

}

func (db *DB) GetMetaCheckpoints(filename string) ([]string, error) {
	checkpoints := []string{}
	q := `SELECT md5 FROM metacheckpoints WHERE filename=$1 ORDER BY idx`
	err := db.Select(&checkpoints, q, filename)
	return checkpoints, err
}

func (db *DB) UpsertMigration(filename, checksum string) error {
	q := `
		INSERT INTO meta (filename, md5) VALUES ($1, $2)
		ON CONFLICT UPDATE md5=$3`
	_, err := db.Exec(q, filename, checksum, checksum)
	return err
}

func (db *DB) InsertMetaCheckpoint(filename, checksum string, idx int) error {
	q := `
		INSERT INTO metacheckpoints (filename, idx, md5)
		VALUES ($1, $2, $3)`
	_, err := db.Exec(q, filename, idx, checksum)
	return err
}

func (db *DB) InsertMigration(filename, checksum string) error {
	q := `INSERT INTO meta (filename, md5) VALUES ($1, $2)`
	_, err := db.Exec(q, filename, checksum)
	return err
}

func (db *DB) DeleteMetaCheckpoints() error {
	q := `DELETE FROM metacheckpoints`
	_, err := db.Exec(q)
	return err
}

func (db *DB) Open() error {
	var err error
	db.DB, err = sqlx.Open("sqlite3", db.filepath)
	if err != nil {
		return errors.Wrap(err, "open db connection")
	}
	return nil
}
