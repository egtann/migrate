package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"

	"github.com/egtann/migrate"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type DB struct {
	connURL   string
	tlsConfig *tlsConfig

	// Embed the sqlx DB struct
	*sqlx.DB
}

func New(
	user, pass, host, dbName string,
	port int,
	sslKey, sslCert, sslCA, sslServerName string,
) (*DB, error) {
	db := &DB{}
	db.connURL = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", user,
		pass, host, port, dbName)
	if sslKey != "" {
		db.connURL = fmt.Sprintf("%s&tls=%s", db.connURL, sslServerName)
		var err error
		db.tlsConfig, err = newTLSConfig(dbName, sslKey,
			sslCert, sslCA, sslServerName)
		if err != nil {
			return nil, errors.Wrap(err, "new tls config")
		}
	}
	return db, nil
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

func (db *DB) CreateMetaIfNotExists() error {
	q := `CREATE TABLE IF NOT EXISTS meta (
		filename VARCHAR(255) UNIQUE NOT NULL,
		md5 VARCHAR(255) NOT NULL,
		content TEXT NOT NULL,
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
		content TEXT NOT NULL,
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
	q := `
	SELECT filename, content, md5 AS checksum
	FROM meta
	ORDER BY filename * 1`
	err := db.Select(&migrations, q)
	return migrations, err

}

func (db *DB) GetMetaCheckpoints(filename string) ([]string, error) {
	checkpoints := []string{}
	q := `SELECT md5 FROM metacheckpoints WHERE filename=? ORDER BY idx`
	err := db.Select(&checkpoints, q, filename)
	return checkpoints, err
}

func (db *DB) UpsertMigration(filename, content, checksum string) error {
	q := `
		INSERT INTO meta (filename, content, md5) VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE md5=?, content=?`
	_, err := db.Exec(q, filename, content, checksum, checksum, content)
	return err
}

func (db *DB) InsertMetaCheckpoint(
	filename, content, checksum string,
	idx int,
) error {
	q := `
		INSERT INTO metacheckpoints (filename, content, idx, md5)
		VALUES (?, ?, ?, ?)`
	_, err := db.Exec(q, filename, content, idx, checksum)
	return err
}

func (db *DB) InsertMigration(filename, content, checksum string) error {
	q := `INSERT INTO meta (filename, content, md5) VALUES (?, ?, ?)`
	_, err := db.Exec(q, filename, content, checksum)
	return err
}

func (db *DB) DeleteMetaCheckpoints() error {
	q := `DELETE FROM metacheckpoints`
	_, err := db.Exec(q)
	return err
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
	q := `ALTER TABLE meta MODIFY COLUMN md5 NOT NULL`
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
		q = `UPDATE meta SET content=? WHERE filename=?`
		if _, err = tx.Exec(q, m.Content, m.Filename); err != nil {
			err = errors.Wrap(err, "update meta content")
			return
		}
	}
	q = `ALTER TABLE meta MODIFY COLUMN content TEXT NOT NULL`
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

	q = `CREATE TABLE metaversion (version INTEGER)`
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

func (db *DB) Open() error {
	if db.tlsConfig != nil {
		err := mysql.RegisterTLSConfig(db.tlsConfig.ServerName,
			db.tlsConfig.Config)
		if err != nil {
			return errors.Wrap(err, "register tls config")
		}
	}
	var err error
	db.DB, err = sqlx.Open("mysql", db.connURL)
	if err != nil {
		return errors.Wrap(err, "open db connection")
	}
	return nil
}

type tlsConfig struct {
	ServerName string
	Config     *tls.Config
}

func newTLSConfig(
	dbName, keyPath, certPath, caPath, serverName string,
) (*tlsConfig, error) {
	rootCertPool := x509.NewCertPool()
	pem, err := ioutil.ReadFile(caPath)
	if err != nil {
		return nil, errors.Wrap(err, "read sql server cert file")
	}
	if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
		return nil, errors.New("failed to append to pem")
	}
	certs, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, errors.Wrap(err, "load x509 key pair")
	}
	clientCert := []tls.Certificate{certs}
	conf := &tlsConfig{
		ServerName: serverName,
		Config: &tls.Config{
			RootCAs:      rootCertPool,
			Certificates: clientCert,
			ServerName:   serverName,
		},
	}
	return conf, nil
}
