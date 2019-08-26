package mysql

import (
	"crypto/tls"
	"crypto/x509"
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
