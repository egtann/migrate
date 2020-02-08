package postgres

import (
	"database/sql"
	"fmt"

	"github.com/egtann/migrate"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	_ "github.com/lib/pq"
)

type DB struct {
	connURL string

	// Embed the sqlx DB struct
	*sqlx.DB
}

func New(
	user, pass, host, dbName string,
	port int,
	sslKey, sslCert, sslCA string,
) *DB {
	// The trailing space is important
	url := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s ",
		host, port, user, pass, dbName)
	if sslKey == "" {
		url += "sslmode=disable"
	} else {
		url += fmt.Sprintf(
			"sslmode=verify-full sslkey=%s sslcert=%s sslrootcert=%s",
			sslKey, sslCert, sslCA)
	}
	return &DB{connURL: url}
}

func (db *DB) CreateMetaIfNotExists() error {
	q := `CREATE TABLE IF NOT EXISTS meta (
		filename TEXT UNIQUE NOT NULL,
		md5 TEXT NOT NULL,
		content TEXT NOT NULL,
		createdat TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
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
		createdat TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
		PRIMARY KEY (filename, idx)
	)`
	if _, err := db.Exec(q); err != nil {
		return errors.Wrap(err, "create metacheckpoints table")
	}
	return nil
}

func (db *DB) GetMigrations() ([]migrate.Migration, error) {
	migrations := []migrate.Migration{}
	q := `
	SELECT filename, content, md5 AS checksum
	FROM meta
	ORDER BY substring(filename, '^\d+')::int`
	err := db.Select(&migrations, q)
	return migrations, err

}

func (db *DB) GetMetaCheckpoints(filename string) ([]string, error) {
	checkpoints := []string{}
	q := `SELECT md5 FROM metacheckpoints WHERE filename=$1 ORDER BY idx`
	err := db.Select(&checkpoints, q, filename)
	return checkpoints, err
}

func (db *DB) UpsertMigration(filename, content, checksum string) error {
	q := `
		INSERT INTO meta (filename, content, md5) VALUES ($1, $2, $3)
		ON CONFLICT (filename) DO UPDATE SET md5=$4, content=$5`
	_, err := db.Exec(q, filename, content, checksum, checksum, content)
	return err
}

func (db *DB) InsertMetaCheckpoint(
	filename, content, checksum string,
	idx int,
) error {
	q := `
		INSERT INTO metacheckpoints (filename, content, idx, md5)
		VALUES ($1, $2, $3, $4)`
	_, err := db.Exec(q, filename, content, idx, checksum)
	return err
}

func (db *DB) InsertMigration(filename, content, checksum string) error {
	q := `INSERT INTO meta (filename, content, md5) VALUES ($1, $2, $3)`
	_, err := db.Exec(q, filename, content, checksum)
	return err
}

func (db *DB) DeleteMetaCheckpoints() error {
	q := `DELETE FROM metacheckpoints`
	_, err := db.Exec(q)
	return err
}

func (db *DB) CreateMetaVersionIfNotExists() (int, error) {
	q := `CREATE TABLE IF NOT EXISTS metaversion (
		version INTEGER NOT NULL
	)`
	if _, err := db.Exec(q); err != nil {
		return 0, errors.Wrap(err, "create metaversion table")
	}

	var version int
	q = `SELECT version FROM metaversion`
	err := db.Get(&version, q)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, errors.Wrap(err, "get version")
	}
	return version, nil
}

func (db *DB) Open() error {
	var err error
	db.DB, err = sqlx.Open("postgres", db.connURL)
	if err != nil {
		return errors.Wrap(err, "open db connection")
	}
	return nil
}

// UpgradeToV1 migrates existing meta tables to the v1 format. Complete any
// migrations before running this function; this will not succeed if have any
// existing metacheckpoints.
func (db *DB) UpgradeToV1(migrations []migrate.Migration) (err error) {
	// Begin Tx
	tx, err := db.Beginx()
	if err != nil {
		return errors.Wrap(err, "begin tx")
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	// Remove the uniqueness constraint from md5
	q := `ALTER TABLE meta DROP CONSTRAINT meta_md5_key`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "remove md5 unique")
		return
	}

	// Add a content column to record the exact migration that ran
	// alongside the md5, insert the appropriate data, then set not null
	q = `ALTER TABLE meta ADD COLUMN content TEXT`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "add content column")
		return
	}
	for _, m := range migrations {
		q = `UPDATE meta SET content=$1 WHERE filename=$2`
		if _, err = tx.Exec(q, m.Content, m.Filename); err != nil {
			err = errors.Wrap(err, "update meta content")
			return
		}
	}
	q = `ALTER TABLE meta ALTER COLUMN content SET NOT NULL`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "update meta content not null")
		return
	}

	// Add the content column to metacheckpoints
	q = `ALTER TABLE metacheckpoints ADD COLUMN content TEXT NOT NULL`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "add metacheckpoints content")
		return
	}

	q = `CREATE TABLE metaversion (version INTEGER NOT NULL)`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "create metaversion table")
		return
	}
	q = `INSERT INTO metaversion (version) VALUES (1)`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "insert metaversion")
		return
	}
	return nil
}
