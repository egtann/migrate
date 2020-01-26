package sqlite

import (
	"testing"

	"github.com/egtann/migrate"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const checkpointFile = "2.sql"

func TestCreateMetaIfNotExists(t *testing.T) {
	t.Parallel()
	db := newDB()
	if err := db.CreateMetaIfNotExists(); err != nil {
		t.Fatal(err)
	}
	var tmp []int
	if err := db.DB.Select(&tmp, `SELECT 1 FROM meta`); err != nil {
		t.Fatal(err)
	}
}

func TestCreateMetaCheckpointsIfNotExists(t *testing.T) {
	t.Parallel()

	db := newDB()
	err := db.CreateMetaCheckpointsIfNotExists()
	check(t, err)

	var tmp []int
	err = db.DB.Select(&tmp, `SELECT 1 FROM metacheckpoints`)
	check(t, err)
}

func TestGetMigrations(t *testing.T) {
	t.Parallel()
	db := setupDBV1(t)
	ms, err := db.GetMigrations()
	check(t, err)
	if len(ms) != 1 {
		t.Fatal("expected 1 migration")
	}
}

func TestGetMetaCheckpoints(t *testing.T) {
	t.Parallel()
	db := setupDBV1(t)
	mcs, err := db.GetMetaCheckpoints(checkpointFile)
	check(t, err)
	if len(mcs) != 1 {
		t.Fatal("expected 1 checkpoint")
	}
}

func TestUpsertMigration(t *testing.T) {
	t.Parallel()
	db := setupDBV1(t)

	// Test update
	err := db.UpsertMigration("1.sql", "SELECT 1;", "md5")
	check(t, err)

	// Test insert
	err = db.UpsertMigration("3.sql", "SELECT 3;", "md5")
	check(t, err)

	ms, err := db.GetMigrations()
	check(t, err)
	if len(ms) != 2 {
		t.Fatal("expected 2 migrations")
	}
}

func TestInsertMetaCheckpoint(t *testing.T) {
	t.Parallel()
	db := setupDBV1(t)

	err := db.InsertMetaCheckpoint(checkpointFile, "SELECT 3;", "md5", 1)
	check(t, err)

	mcs, err := db.GetMetaCheckpoints(checkpointFile)
	check(t, err)
	if len(mcs) != 2 {
		t.Fatal("expected 2 checkpoints")
	}
}

func TestInsertMigration(t *testing.T) {
	t.Parallel()
	db := setupDBV1(t)

	err := db.InsertMigration("3.sql", "SELECT 3;", "md5")
	check(t, err)

	ms, err := db.GetMigrations()
	check(t, err)
	if len(ms) != 2 {
		t.Fatal("expected 2 migrations")
	}
}

func TestDeleteMetaCheckpoints(t *testing.T) {
	t.Parallel()
	db := setupDBV1(t)

	err := db.DeleteMetaCheckpoints()
	check(t, err)

	mcs, err := db.GetMetaCheckpoints(checkpointFile)
	check(t, err)
	if len(mcs) != 0 {
		t.Fatal("expected 0 checkpoints")
	}
}

func TestUpdateMetaVersion(t *testing.T) {
	t.Parallel()
	db := setupDBV1(t)

	const v = 2
	err := db.UpdateMetaVersion(v)
	check(t, err)

	version, err := db.CreateMetaVersionIfNotExists()
	check(t, err)
	if version != v {
		t.Fatalf("expected version %d", v)
	}
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func newDB() *DB {
	// Every database connection sees a different database, which is
	// perfect, as that lets us run tests in parallel.
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	return &DB{DB: db}
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
		VALUES ($1, $2, $3, $4)`
	_, err = db.DB.Exec(q, 0, checkpointFile, "SELECT 2;", "md5")
	check(t, err)

	return db
}

func setupDBV0(t *testing.T) *DB {
	db := newDB()

	q := `CREATE TABLE IF NOT EXISTS meta (
		filename TEXT UNIQUE NOT NULL,
		md5 TEXT UNIQUE NOT NULL,
		createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`
	_, err := db.DB.Exec(q)
	check(t, err)

	q = `CREATE TABLE IF NOT EXISTS metacheckpoints (
		filename TEXT NOT NULL,
		idx INTEGER NOT NULL,
		md5 TEXT NOT NULL,
		createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (filename, idx)
	)`
	_, err = db.DB.Exec(q)
	check(t, err)

	q = `INSERT INTO meta (filename, md5) VALUES ($1, $2)`
	_, err = db.DB.Exec(q, "1.sql", "md5")
	check(t, err)

	return db
}
