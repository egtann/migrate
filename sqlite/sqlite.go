package sqlite

import (
	"database/sql"

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
		content TEXT NOT NULL,
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
		content TEXT NOT NULL,
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
	q := `SELECT filename, content, md5 AS checksum FROM meta`
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
		ON CONFLICT(filename) DO UPDATE SET md5=$4, content=$5`
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
	db.DB, err = sqlx.Open("sqlite3", db.filepath)
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

	// Remove the uniqueness constraint from md5. sqlite doesn't support
	// MODIFY COLUMN so we recreate the table.
	q := `CREATE TABLE metatmp (
		filename TEXT UNIQUE NOT NULL,
		md5 TEXT NOT NULL,
		createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "create metatmp")
		return
	}
	q = `INSERT INTO metatmp SELECT filename, md5, createdat FROM meta`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "insert metatmp")
	}
	q = `DROP TABLE meta`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "drop meta")
	}
	q = `ALTER TABLE metatmp RENAME TO meta`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "rename metatmp 1")
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

	// Once again, sqlite3 doesn't support modify column, so we have to
	// recreate our tables
	q = `CREATE TABLE metatmp (
		filename TEXT UNIQUE NOT NULL,
		content TEXT NOT NULL,
		md5 TEXT NOT NULL,
		createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "create metatmp")
		return
	}
	q = `
		INSERT INTO metatmp
		SELECT filename, content, md5, createdat FROM meta`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "")
	}
	q = `DROP TABLE meta`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "drop meta")
		return
	}
	q = `ALTER TABLE metatmp RENAME TO meta`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "rename metatmp 2")
		return
	}

	// Add the content column to metacheckpoints. Same song and dance as above
	q = `CREATE TABLE metacheckpointstmp (
		filename TEXT NOT NULL,
		content TEXT NOT NULL,
		idx INTEGER NOT NULL,
		md5 TEXT NOT NULL,
		createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (filename, idx)
	)`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "create metacheckpointstmp")
		return
	}
	q = `
		INSERT INTO metacheckpointstmp
		SELECT filename, md5, createdat FROM meta`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "insert metacheckpointstmp")
	}
	q = `DROP TABLE metacheckpoints`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "drop metacheckpoints")
		return
	}
	q = `ALTER TABLE metacheckpointstmp RENAME TO metacheckpoints`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "rename metacheckpointstmp")
		return
	}

	q = `CREATE TABLE metaversion (version INTEGER);`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "create metaversion table")
		return
	}
	q = `INSERT INTO metaversion (version) VALUES (1)`
	if _, err = tx.Exec(q); err != nil {
		err = errors.Wrap(err, "update metaversion")
		return
	}
	return nil
}
