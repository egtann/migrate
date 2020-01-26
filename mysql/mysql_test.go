package mysql

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/egtann/migrate"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const checkpointFile = "2.sql"

func TestMain(m *testing.M) {
	path := filepath.Join("..", "test.env")
	err := parseEnv(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse %s: %s\n", path, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestCreateMetaIfNotExists(t *testing.T) {
	db := newDB(t)
	defer teardown(t, db)

	err := db.CreateMetaIfNotExists()
	check(t, err)

	var tmp []int
	err = db.DB.Select(&tmp, `SELECT 1 FROM meta`)
	check(t, err)
}

func TestCreateMetaCheckpointsIfNotExists(t *testing.T) {
	db := newDB(t)
	defer teardown(t, db)

	err := db.CreateMetaCheckpointsIfNotExists()
	check(t, err)

	var tmp []int
	err = db.DB.Select(&tmp, `SELECT 1 FROM metacheckpoints`)
	check(t, err)
}

func TestGetMigrations(t *testing.T) {
	db := setupDBV1(t)
	defer teardown(t, db)

	ms, err := db.GetMigrations()
	check(t, err)
	if len(ms) != 1 {
		t.Fatal("expected 1 migration")
	}
}

func TestGetMetaCheckpoints(t *testing.T) {
	db := setupDBV1(t)
	defer teardown(t, db)

	mcs, err := db.GetMetaCheckpoints(checkpointFile)
	check(t, err)
	if len(mcs) != 1 {
		t.Fatal("expected 1 checkpoint")
	}
}

func TestUpsertMigration(t *testing.T) {
	db := setupDBV1(t)
	defer teardown(t, db)

	// Test update
	err := db.UpsertMigration("1.sql", "SELECT 1;", "md5")
	check(t, err)

	// Test insert
	err = db.UpsertMigration("3.sql", "SELECT 3;", "md5")
	check(t, err)

	ms, err := db.GetMigrations()
	check(t, err)
	if len(ms) != 2 {
		t.Fatalf("expected 2 migrations, got %d", len(ms))
	}
}

func TestInsertMetaCheckpoint(t *testing.T) {
	db := setupDBV1(t)
	defer teardown(t, db)

	err := db.InsertMetaCheckpoint(checkpointFile, "SELECT 3;", "md5", 1)
	check(t, err)

	mcs, err := db.GetMetaCheckpoints(checkpointFile)
	check(t, err)
	if len(mcs) != 2 {
		t.Fatal("expected 2 checkpoints")
	}
}

func TestInsertMigration(t *testing.T) {
	db := setupDBV1(t)
	defer teardown(t, db)

	err := db.InsertMigration("3.sql", "SELECT 3;", "md5")
	check(t, err)

	ms, err := db.GetMigrations()
	check(t, err)
	if len(ms) != 2 {
		t.Fatal("expected 2 migrations")
	}
}

func TestDeleteMetaCheckpoints(t *testing.T) {
	db := setupDBV1(t)
	defer teardown(t, db)

	err := db.DeleteMetaCheckpoints()
	check(t, err)

	mcs, err := db.GetMetaCheckpoints(checkpointFile)
	check(t, err)
	if len(mcs) != 0 {
		t.Fatal("expected 0 checkpoints")
	}
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func must(err error) {
}

func newDB(t *testing.T) *DB {
	db := createDBAndOpen(t)
	return &DB{DB: db}
}

func parseEnv(filename string) error {
	fi, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fi.Close()

	scn := bufio.NewScanner(fi)
	for i := 1; scn.Scan(); i++ {
		line := scn.Text()
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("bad line %d: %s", i, line)
		}
		if err = os.Setenv(parts[0], parts[1]); err != nil {
			return errors.Wrap(err, "set env")
		}
	}
	if err = scn.Err(); err != nil {
		return errors.Wrap(err, "scan")
	}
	return nil
}

func createDBAndOpen(t *testing.T) *sqlx.DB {
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		panic("missing MYSQL_USER")
	}
	pass := os.Getenv("MYSQL_PASSWORD")
	if pass == "" {
		panic("missing MYSQL_PASSWORD")
	}
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		panic("missing MYSQL_HOST")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?timeout=1s", user, pass, host)
	db, err := sqlx.Open("mysql", dsn)
	check(t, err)

	q := `DROP DATABASE IF EXISTS migrate_test`
	_, err = db.Exec(q)
	check(t, err)

	q = `CREATE DATABASE migrate_test`
	_, err = db.Exec(q)
	check(t, err)

	err = db.Close()
	check(t, err)

	dsn = fmt.Sprintf("%s:%s@tcp(%s)/migrate_test?timeout=1s", user, pass,
		host)
	db, err = sqlx.Open("mysql", dsn)
	check(t, err)

	return db
}

func teardown(t *testing.T, db *DB) {
	q := `DROP DATABASE migrate_test`
	_, err := db.Exec(q)
	check(t, err)
}

func setupDBV1(t *testing.T) *DB {
	db := setupDBV0(t)
	err := db.UpgradeToV1([]migrate.Migration{{
		Filename: "1.sql",
		Checksum: "md5",
		Content:  "SELECT 1;",
	}})
	check(t, err)

	q := `
		INSERT INTO metacheckpoints (idx, filename, content, md5)
		VALUES (?, ?, ?, ?)`
	_, err = db.DB.Exec(q, 0, checkpointFile, "SELECT 2;", "md5")
	check(t, err)

	return db
}

func setupDBV0(t *testing.T) *DB {
	db := newDB(t)

	q := `CREATE TABLE IF NOT EXISTS meta (
		filename VARCHAR(255) UNIQUE NOT NULL,
		md5 VARCHAR(255) UNIQUE NOT NULL,
		createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`
	_, err := db.DB.Exec(q)
	check(t, err)

	q = `CREATE TABLE IF NOT EXISTS metacheckpoints (
		filename VARCHAR(255) NOT NULL,
		idx INTEGER NOT NULL,
		md5 VARCHAR(255) NOT NULL,
		createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (filename, idx)
	)`
	_, err = db.DB.Exec(q)
	check(t, err)

	q = `INSERT INTO meta (filename, md5) VALUES (?, ?)`
	_, err = db.DB.Exec(q, "1.sql", "md5")
	check(t, err)

	return db
}
