package main

import (
	"flag"
	"fmt"
	"os"
	"syscall"

	"github.com/egtann/migrate"
	"github.com/egtann/migrate/mysql"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh/terminal"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	migrationDir := flag.String("dir", ".", "migrations directory")
	dbName := flag.String("db", "", "database name")
	dbUser := flag.String("u", "root", "database user")
	dbHost := flag.String("h", "127.0.0.1", "database host")
	dbPort := flag.Int("p", 3306, "database port")
	dbType := flag.String("t", "mysql", "type of database (mysql, postgres, sqlite)")
	dry := flag.Bool("d", false, "dry run")
	sslKey := flag.String("ssl-key", "", "path to client key pem")
	sslCert := flag.String("ssl-cert", "", "path to client cert pem")
	sslCA := flag.String("ssl-ca", "", "path to server ca pem")
	sslServerName := flag.String("ssl-server", "", "server name for ssl")
	skip := flag.String("skip", "", "skip up to this filename (inclusive)")
	pass := flag.String("pass", "", "password (optional flag, if not provided it will be requested)")
	flag.Parse()
	if len(*dbName) == 0 {
		return errors.New("database name cannot be empty. specify using the -db flag. run `migrate -h` for help")
	}
	if *dry && *skip != "" {
		return errors.New("cannot skip ahead with dry mode")
	}

	// Attempt to use ssl if any ssl flags are defined
	ssl := *sslKey != "" || *sslCert != "" || *sslCA != "" || *sslServerName != ""

	// Request database password if not provided as a flag argument
	var password []byte
	if len(*pass) == 0 {
		fmt.Printf("%s database password: ", *dbName)
		var err error
		password, err = terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return errors.Wrap(err, "read pass")
		}
		fmt.Printf("\n")
	} else {
		password = []byte(*pass)
	}

	// Connect to the database
	var tlsConf *migrate.TLSConfig
	pth := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", *dbUser,
		string(password), *dbHost, *dbPort, *dbName)
	if ssl {
		fmt.Println("using tls")
		var err error
		tlsConf, err = migrate.NewTLSConfig(*dbName, *sslKey, *sslCert,
			*sslCA, *sslServerName)
		if err != nil {
			return errors.Wrap(err, "new tls config")
		}
	}

	// This is the only line that needs to change
	var db migrate.Store
	switch *dbType {
	case "mysql":
		db = &mysql.DB{}
	case "sqlite": // TODO
	case "postgres": // TODO
	default:
		return fmt.Errorf("unknown db type: %s", *dbType)
	}
	if err := db.Open(pth, tlsConf); err != nil {
		return errors.Wrap(err, "open")
	}

	// Get files in migration dir and sort them
	files, err := migrate.ReadDir(*migrationDir)
	if err != nil {
		return errors.Wrap(err, "get migrations")
	}
	if err = migrate.Sort(files); err != nil {
		return errors.Wrap(err, "sort")
	}

	// Create meta tables if we need to, so we can store the migration
	// state in the db itself
	if err = db.CreateMetaIfNotExists(); err != nil {
		return errors.Wrap(err, "create meta table")
	}
	if err = db.CreateMetaCheckpointsIfNotExists(); err != nil {
		return errors.Wrap(err, "create meta checkpoints table")
	}

	// If skip, then we record the migrations but do not perform them. This
	// enables you to start using this package on an existing database
	index := 0
	if len(*skip) > 0 {
		index, err = migrate.SkipAhead(db, files, *migrationDir, *skip)
		if err != nil {
			return errors.Wrap(err, "skip ahead")
		}
		fmt.Println("skipped ahead")
	}

	// Get all migrations
	migrations, err := db.GetMigrations()
	if err != nil {
		return errors.Wrap(err, "get migrations")
	}
	err = migrate.ValidHistory(*migrationDir, files, migrations, index)
	if err != nil {
		return errors.Wrap(err, "valid history")
	}

	if *dry {
		if len(migrations) == len(files) {
			fmt.Println("up to date")
			return nil
		}
		for i := len(migrations); i < len(files); i++ {
			fmt.Println("would migrate", files[i].Name())
		}
		return nil
	}

	migrated, err := migrate.Migrate(db, files, migrations)
	if err != nil {
		return errors.Wrap(err, "migrated")
	}

	// Check for any files in the directory beyond what we see in the
	// migration history. These are new files. Migrate each of them
	q = `DROP TABLE metacheckpoints`
	_, err = db.Exec(q)
	if err != nil {
		return errors.Wrap(err, "drop metacheckpoints table")
	}
	if migrated {
		fmt.Println("success")
	} else {
		fmt.Println("up to date")
	}
	return nil
}
