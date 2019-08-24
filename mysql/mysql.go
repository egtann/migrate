package mysql

import (
	"github.com/egtann/migrate"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type DB struct{ *sqlx.DB }

func (db *DB) CreateMetaIfNotExists() error {
	q := `CREATE TABLE IF NOT EXISTS meta (
		filename VARCHAR(255) UNIQUE NOT NULL,
		md5 VARCHAR(255) UNIQUE NOT NULL,
		createdat DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
	)`
	if _, err := db.Exec(q); err != nil {
		return errors.Wrap(err, "create meta table")
	}
	return nil
}

func (db *DB) CreateMetaCheckpointsIfNotExists() error {
	q := `CREATE TABLE IF NOT EXISTS metacheckpoints (
		filename VARCHAR(255) NOT NULL,
		idx INTEGER NOT NULL,
		md5 VARCHAR(255) NOT NULL,
		createdat DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		PRIMARY KEY (filename, idx)
	)`
	if _, err := db.Exec(q); err != nil {
		return errors.Wrap(err, "create metacheckpoints table")
	}
	return nil
}

func (db *DB) GetMigrations() ([]migrate.Migration, error) {
	migrations := []migrate.Migration{}
	q := `SELECT filename, md5 AS checksum FROM meta ORDER BY filename * 1`
	err := db.Select(&migrations, q)
	return migrations, err

}

func (db *DB) GetMetaCheckpoints(filename string) ([]string, error) {
	checkpoints := []string{}
	q := `SELECT md5 FROM metacheckpoints WHERE filename=? ORDER BY idx`
	err := db.Select(&checkpoints, q, filename)
	return checkpoints, err
}

func (db *DB) UpsertMigration(filename, checksum string) error {
	q := `
		INSERT INTO meta (filename, md5) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE md5=?`
	_, err := db.Exec(q, filename, checksum, checksum)
	return err
}

func (db *DB) InsertMetaCheckpoint(filename, checksum string, idx int) error {
	q := `
		INSERT INTO metacheckpoints (filename, idx, md5)
		VALUES (?, ?, ?)`
	_, err := db.Exec(q, filename, idx, checksum)
	return err
}

func (db *DB) InsertMigration(filename, checksum string) error {
	q := `INSERT INTO meta (filename, md5) VALUES (?, ?)`
	_, err := db.Exec(q, filename, checksum)
	return err
}

func (db *DB) DeleteMetaCheckpoints() error {
	q := `DELETE FROM metacheckpoints`
	_, err := db.Exec(q)
	return err
}

func (db *DB) DropMetaCheckpoints() error {
	q := `DROP TABLE metacheckpoints`
	_, err := db.Exec(q)
	return err
}

func (db *DB) Open(connURL string, conf *migrate.TLSConfig) error {
	err := mysql.RegisterTLSConfig(conf.DBName, conf.Config)
	if err != nil {
		return errors.Wrap(err, "register tls config")
	}
	connURL += "&tls=" + conf.DBName
	db.DB, err = sqlx.Open("mysql", connURL)
	if err != nil {
		return errors.Wrap(err, "open db connection")
	}
	return nil
}
