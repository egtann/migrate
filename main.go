package main

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh/terminal"
)

type migration struct {
	Filename string
	Checksum string
}

func main() {
	log.SetFlags(0)

	migrationDir := flag.String("dir", ".", "migrations directory")
	dbName := flag.String("db", "", "database name")
	dbUser := flag.String("u", "root", "database user")
	dbHost := flag.String("h", "127.0.0.1", "database host")
	dbPort := flag.Int("p", 3306, "database port")
	sslKey := flag.String("ssl-key", "", "path to client key pem")
	sslCert := flag.String("ssl-cert", "", "path to client cert pem")
	sslCA := flag.String("ssl-ca", "", "path to server ca pem")
	sslServerName := flag.String("ssl-server", "", "server name for ssl")
	skip := flag.String("skip", "", "skip up to this filename (inclusive)")
	pass := flag.String("pass", "", "password (optional flag, if not provided it will be requested)")
	flag.Parse()
	if len(*dbName) == 0 {
		log.Fatal("database name cannot be empty. specify using the -db flag. run `migrate -h` for help")
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
			log.Fatal(err)
		}
		log.Printf("\n")
	} else {
		password = []byte(*pass)
	}

	// Connect to the database
	pth := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", *dbUser,
		string(password), *dbHost, *dbPort, *dbName)
	if ssl {
		err := registerTLSConfig(*dbName, *sslKey, *sslCert, *sslCA, *sslServerName)
		if err != nil {
			log.Fatal(err)
		}
		pth += "&tls=" + *dbName
	}

	db, err := sqlx.Open("mysql", pth)
	if err != nil {
		log.Fatal(err)
	}

	// Get files in migration dir
	files := []os.FileInfo{}
	tmp, err := ioutil.ReadDir(*migrationDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, fi := range tmp {
		// Skip directories and hidden files
		if fi.IsDir() || strings.HasPrefix(fi.Name(), ".") {
			continue
		}
		files = append(files, fi)
	}

	// Sort the files by name, ensuring that something like 1.sql, 2.sql,
	// 10.sql is correct
	regexNum := regexp.MustCompile(`^\d+`)
	sort.Slice(files, func(i, j int) bool {
		fiName1 := regexNum.FindString(files[i].Name())
		fiName2 := regexNum.FindString(files[j].Name())
		fiNum1, err := strconv.ParseUint(fiName1, 10, 64)
		if err != nil {
			err = errors.Wrapf(err, "parse uint in %s", files[i].Name())
			log.Fatal(err)
		}
		fiNum2, err := strconv.ParseUint(fiName2, 10, 64)
		if err != nil {
			err = errors.Wrapf(err, "parse uint in %s", files[i].Name())
			log.Fatal(err)
		}
		return fiNum1 < fiNum2
	})

	// Create meta tables if we need to, so we can store the migration
	// state in the db itself
	q := `CREATE TABLE IF NOT EXISTS meta (
		filename VARCHAR(255) UNIQUE NOT NULL,
		md5 VARCHAR(255) UNIQUE NOT NULL,
		createdat DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
	)`
	if _, err = db.Exec(q); err != nil {
		log.Fatal(errors.Wrap(err, "create meta table"))
	}
	q = `CREATE TABLE IF NOT EXISTS metacheckpoints (
		filename VARCHAR(255) NOT NULL,
		idx INTEGER NOT NULL,
		md5 VARCHAR(255) NOT NULL,
		createdat DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		PRIMARY KEY (filename, idx)
	)`
	if _, err = db.Exec(q); err != nil {
		log.Fatal(errors.Wrap(err, "create metacheckpoints table"))
	}

	// If skip, then we record the migrations but do not perform them. This
	// enables you to start using this package on an existing database
	index := 0
	if len(*skip) > 0 {
		index, err = skipAhead(db, files, *migrationDir, *skip)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("skipped ahead")
	}

	// Get all migrations
	migrations := []migration{}
	q = `SELECT filename, md5 AS checksum FROM meta ORDER BY filename * 1`
	if err = db.Select(&migrations, q); err != nil {
		log.Fatal(err)
	}
	for i := index; i < len(migrations); i++ {
		m := migrations[i]
		if m.Filename != files[i].Name() {
			log.Printf("\n%s was added to history before %s.",
				files[i].Name(), m.Filename)
			log.Fatal("failed to migrate. migrations must be appended")
		}
		err = checkHash(*migrationDir, m.Filename, m.Checksum)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Check for any files in the directory beyond what we see in the
	// migration history. These are new files. Migrate each of them
	var migrated bool
	for i := len(migrations); i < len(files); i++ {
		filename := files[i].Name()
		err = migrate(db, *migrationDir, filename)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("migrated", filename)
		migrated = true
	}
	q = `DROP TABLE metacheckpoints`
	_, err = db.Exec(q)
	if err != nil {
		log.Fatal(errors.Wrap(err, "drop metacheckpoints table"))
	}
	if migrated {
		log.Println("success")
	} else {
		log.Println("up to date")
	}
}

func checkHash(baseDir, filename, checksum string) error {
	fi, err := os.Open(filepath.Join(baseDir, filename))
	if err != nil {
		return err
	}
	defer fi.Close()
	check, err := computeChecksum(fi)
	if err != nil {
		return err
	}
	if check != checksum {
		log.Println("comparing", check, checksum)
		return fmt.Errorf("checksum does not match %s. has the file changed?", filename)
	}
	return nil
}

func migrate(db *sqlx.DB, baseDir, filename string) error {
	pth := filepath.Join(baseDir, filename)
	byt, err := ioutil.ReadFile(pth)
	if err != nil {
		return err
	}
	if err = migrateSQL(db, baseDir, filename, byt); err != nil {
		return errors.Wrapf(err, "migrate %s", filename)
	}
	checksum, err := computeChecksum(bytes.NewReader(byt))
	if err != nil {
		return errors.Wrap(err, "compute file checksum")
	}
	const q = `INSERT INTO meta (filename, md5) VALUES (?, ?)`
	_, err = db.Exec(q, filename, checksum)
	return errors.Wrap(err, "insert meta")
}

func migrateSQL(db *sqlx.DB, baseDir, filename string, byt []byte) error {
	cmds := strings.Split(string(byt), ";")
	filteredCmds := []string{}
	for _, cmd := range cmds {
		cmd = strings.TrimSpace(cmd)
		if len(cmd) > 0 && !strings.HasPrefix(cmd, "--") {
			filteredCmds = append(filteredCmds, cmd)
		}
	}

	// Get our checkpoints, if any
	checkpoints := []string{}
	q := `SELECT md5 FROM metacheckpoints WHERE filename=? ORDER BY idx`
	err := db.Select(&checkpoints, q, filename)
	if err != nil {
		return errors.Wrap(err, "get metacheckpoints")
	}
	if len(checkpoints) > 0 {
		log.Printf("found %d checkpoints\n", len(checkpoints))
	}

	// Ensure commands weren't deleted from the file after we migrated them
	if len(checkpoints) >= len(filteredCmds) {
		return fmt.Errorf("len(checkpoints) %d >= len(cmds) %d",
			len(checkpoints), len(filteredCmds))
	}

	for i, cmd := range filteredCmds {
		// Confirm the file up to our checkpoint has not changed
		if i < len(checkpoints) {
			r := strings.NewReader(cmd)
			checksum, err := computeChecksum(r)
			if err != nil {
				return errors.Wrap(err, "compute checkpoint checksum")
			}
			if checksum != checkpoints[i] {
				return fmt.Errorf("checksum does not equal checkpoint. has %s (cmd %d) changed?",
					filename, i)
			}
			continue
		}

		// Execute non-checkpointed commands one by one
		_, err := db.Exec(cmd)
		if err != nil {
			log.Println("failed on", cmd)
			return fmt.Errorf("%s: %s", filename, err)
		}

		// Save a checkpoint
		checksum, err := computeChecksum(strings.NewReader(cmd))
		if err != nil {
			return errors.Wrap(err, "compute checksum")
		}
		q = `INSERT INTO metacheckpoints (filename, idx, md5) VALUES (?, ?, ?)`
		_, err = db.Exec(q, filename, i, checksum)
		if err != nil {
			return errors.Wrap(err, "insert meta")
		}
	}

	// We've successfully finished migrating the file, so we delete the
	// temporary progress in metacheckpoints
	q = `DELETE FROM metacheckpoints`
	if _, err = db.Exec(q); err != nil {
		return errors.Wrap(err, "insert meta")
	}
	return nil
}

func computeChecksum(r io.Reader) (string, error) {
	h := md5.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func skipAhead(
	db *sqlx.DB,
	files []os.FileInfo,
	baseDir, skipToFile string,
) (int, error) {
	// Get just the filename if skip is a directory
	_, skipToFile = path.Split(skipToFile)

	// Ensure the file exists
	index := -1
	for i, fi := range files {
		if fi.Name() == skipToFile {
			index = i
			break
		}
	}
	if index == -1 {
		log.Fatalf("%s does not exist", skipToFile)
	}
	const q = `
		INSERT INTO meta (filename, md5) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE md5=?`
	for i := 0; i <= index; i++ {
		name := files[i].Name()
		fi, err := os.Open(filepath.Join(baseDir, name))
		if err != nil {
			return -1, err
		}
		checksum, err := computeChecksum(fi)
		if err != nil {
			fi.Close()
			return -1, err
		}
		_, err = db.Exec(q, name, checksum, checksum)
		if err != nil {
			fi.Close()
			return -1, err
		}
		if err = fi.Close(); err != nil {
			return -1, err
		}
	}
	return index, nil
}

func registerTLSConfig(dbName, certPath, keyPath, caPath, serverName string) error {
	rootCertPool := x509.NewCertPool()
	pem, err := ioutil.ReadFile(caPath)
	if err != nil {
		return errors.Wrap(err, "read sql server cert file")
	}
	if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
		return errors.New("failed to append to pem")
	}
	clientCert := make([]tls.Certificate, 0, 1)
	certs, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return errors.Wrap(err, "load x509 key pair")
	}
	clientCert = append(clientCert, certs)
	err = mysql.RegisterTLSConfig(dbName, &tls.Config{
		RootCAs:      rootCertPool,
		Certificates: clientCert,
		ServerName:   serverName,
	})
	return errors.Wrap(err, "register tls config")
}
