package main

import (
	"flag"
	"fmt"
	"os"
	"syscall"

	"github.com/egtann/migrate"
	"github.com/egtann/migrate/mysql"
	"github.com/egtann/migrate/postgres"
	"github.com/egtann/migrate/sqlite"
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
	dbUser := flag.String("u", "", "database user")
	dbHost := flag.String("h", "127.0.0.1", "database host")
	dbPort := flag.Int("p", 0, "database port")
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

	// Validate flags for each type of database and set appropriate
	// defaults
	switch *dbType {
	case "sqlite":
		if *dbUser != "" {
			return errors.New("sqlite does not support the -u flag")
		}
		if *dbHost != "127.0.0.1" {
			return errors.New("sqlite does not support the -h flag")
		}
		if *dbPort != 0 {
			return errors.New("sqlite does not support the -p flag")
		}
		if *pass != "" {
			return errors.New("sqlite does not support the -pass flag")
		}
		if *sslKey != "" || *sslCert != "" || *sslCA != "" || *sslServerName != "" {
			return errors.New("sqlite does not support ssl")
		}
	case "postgres":
		if *sslServerName != "" {
			return errors.New("postgres does not support the -ssl-server flag")
		}
		if *dbUser == "" {
			*dbUser = "postgres"
		}
		if *dbPort == 0 {
			*dbPort = 5432
		}
	case "mysql":
		if *dbUser == "" {
			*dbUser = "root"
		}
		if *dbPort == 0 {
			*dbPort = 3306
		}
	default:
		return fmt.Errorf("unknown db type %s (mysql, postgres, sqlite allowed)")
	}

	// Request database password if not provided as a flag argument
	var password []byte
	if *dbType != "sqlite" {
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
	}

	// Prepare our database-specific configs
	var db migrate.Store
	switch *dbType {
	case "mysql":
		var err error
		db, err = mysql.New(*dbUser, string(password), *dbHost,
			*dbName, *dbPort, *sslKey, *sslCert, *sslCA,
			*sslServerName)
		if err != nil {
			return errors.Wrap(err, "mysql new")
		}
	case "sqlite":
		db = sqlite.New(*dbName)
	case "postgres":
		db = postgres.New(*dbUser, string(password), *dbHost, *dbName,
			*dbPort, *sslKey, *sslCert, *sslCA)
	default:
		return fmt.Errorf("unknown db type: %s", *dbType)
	}
	if *sslKey != "" {
		fmt.Println("using tls")
	}
	if err := db.Open(); err != nil {
		return errors.Wrap(err, "open")
	}

	// Prepare our database for migrations and collect the relevant files.
	m, err := migrate.New(db, migrate.StdLogger{}, *migrationDir, *skip)
	if err != nil {
		return err
	}
	if *dry {
		if len(m.Migrations) == len(m.Files) {
			fmt.Println("up to date")
			return nil
		}
		for i := len(m.Migrations); i < len(m.Files); i++ {
			fmt.Println("would migrate", m.Files[i].Name())
		}
		return nil
	}
	migrated, err := m.Migrate()
	if err != nil {
		return err
	}
	if migrated {
		fmt.Println("success")
	} else {
		fmt.Println("up to date")
	}
	return nil
}
