package migrate

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type Store interface {
	Open(connURL string, conf *TLSConfig) error
	Exec(string, ...interface{}) (sql.Result, error)

	CreateMetaIfNotExists() error
	CreateMetaCheckpointsIfNotExists() error

	GetMigrations() ([]Migration, error)
	InsertMigration(filename, checksum string) error
	UpsertMigration(filename, checksum string) error

	GetMetaCheckpoints(string) ([]string, error)
	InsertMetaCheckpoint(filename, checksum string, idx int) error
	DeleteMetaCheckpoints() error
}

type TLSConfig struct {
	DBName string
	Config *tls.Config
}

type Migration struct {
	Filename string
	Checksum string
}

var regexNum = regexp.MustCompile(`^\d+`)

// ReadDir collects file infos from the migration directory.
func ReadDir(dir string) ([]os.FileInfo, error) {
	files := []os.FileInfo{}
	tmp, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "read dir")
	}
	for _, fi := range tmp {
		// Skip directories and hidden files
		if fi.IsDir() || strings.HasPrefix(fi.Name(), ".") {
			continue
		}
		// Skip any non-sql files
		if filepath.Ext(fi.Name()) != ".sql" {
			continue
		}
		files = append(files, fi)
	}
	if len(files) == 0 {
		return nil, errors.New("no sql migration files found (might be the wrong -dir)")
	}
	return files, nil
}

// Sort the files by name, ensuring that something like 1.sql, 2.sql, 10.sql is
// correct
func Sort(files []os.FileInfo) error {
	var nameErr error
	sort.Slice(files, func(i, j int) bool {
		if nameErr != nil {
			return false
		}
		fiName1 := regexNum.FindString(files[i].Name())
		fiName2 := regexNum.FindString(files[j].Name())
		fiNum1, err := strconv.ParseUint(fiName1, 10, 64)
		if err != nil {
			nameErr = errors.Wrapf(err, "parse uint in file %s", files[i].Name())
			return false
		}
		fiNum2, err := strconv.ParseUint(fiName2, 10, 64)
		if err != nil {
			nameErr = errors.Wrapf(err, "parse uint in file %s", files[i].Name())
			return false
		}
		if fiNum1 == fiNum2 {
			nameErr = fmt.Errorf("cannot have duplicate timestamp: %d", fiNum1)
			return false
		}
		return fiNum1 < fiNum2
	})
	return nameErr
}

func ValidHistory(
	dir string,
	files []os.FileInfo,
	migrations []Migration,
	index int,
) error {
	for i := len(files); i < len(migrations); i++ {
		fmt.Printf("missing already-run migration %q\n", migrations[i])
	}
	if len(files) < len(migrations) {
		return errors.New("cannot continue with missing migrations")
	}
	for i := index; i < len(migrations); i++ {
		m := migrations[i]
		if m.Filename != files[i].Name() {
			fmt.Printf("\n%s was added to history before %s.",
				files[i].Name(), m.Filename)
			return errors.New("failed to migrate. migrations must be appended")
		}
		err := checkHash(dir, m.Filename, m.Checksum)
		if err != nil {
			return errors.Wrap(err, "check hash")
		}
	}
	return nil
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
		fmt.Println("comparing", check, checksum)
		return fmt.Errorf("checksum does not match %s. has the file changed?", filename)
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

// Migrate reports whether any migration took place and whether everything
// succeeded.
func Migrate() (bool, error) {
	var migrated bool
	for i := len(migrations); i < len(files); i++ {
		filename := files[i].Name()
		err = migrateFile(db, *migrationDir, filename)
		if err != nil {
			return errors.Wrap(err, "migrate")
		}
		fmt.Println("migrated", filename)
		migrated = true
	}
}

func migrateFile(db Store, baseDir, filename string) error {
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
	if err = db.InsertMigration(filename, checksum); err != nil {
		return errors.Wrap(err, "insert migration")
	}
	return nil
}

func migrateSQL(db Store, baseDir, filename string, byt []byte) error {
	cmds := strings.Split(string(byt), ";")
	filteredCmds := []string{}
	for _, cmd := range cmds {
		cmd = strings.TrimSpace(cmd)
		if len(cmd) > 0 && !strings.HasPrefix(cmd, "--") {
			filteredCmds = append(filteredCmds, cmd)
		}
	}

	// Get our checkpoints, if any
	checkpoints, err := db.GetMetaCheckpoints(filename)
	if err != nil {
		return errors.Wrap(err, "get checkpoints")
	}
	if len(checkpoints) > 0 {
		fmt.Printf("found %d checkpoints\n", len(checkpoints))
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
			fmt.Println("failed on", cmd)
			return fmt.Errorf("%s: %s", filename, err)
		}

		// Save a checkpoint
		checksum, err := computeChecksum(strings.NewReader(cmd))
		if err != nil {
			return errors.Wrap(err, "compute checksum")
		}
		err = db.InsertMetaCheckpoint(filename, checksum, i)
		if err != nil {
			return errors.Wrap(err, "insert checkpoint")
		}
	}

	// We've successfully finished migrating the file, so we delete the
	// temporary progress in metacheckpoints
	if err = db.DeleteMetaCheckpoints(); err != nil {
		return errors.Wrap(err, "delete checkpoints")
	}
	return nil
}

func SkipAhead(
	db Store,
	files []os.FileInfo,
	baseDir, skipToFile string,
) (int, error) {
	// Get just the filename if skip is a directory
	_, skipToFile = filepath.Split(skipToFile)

	// Ensure the file exists
	index := -1
	for i, fi := range files {
		if fi.Name() == skipToFile {
			index = i
			break
		}
	}
	if index == -1 {
		return 0, fmt.Errorf("%s does not exist", skipToFile)
	}
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
		if err = db.UpsertMigration(name, checksum); err != nil {
			fi.Close()
			return -1, err
		}
		if err = fi.Close(); err != nil {
			return -1, err
		}
	}
	return index, nil
}

func NewTLSConfig(dbName, keyPath, certPath, caPath, serverName string) (*TLSConfig, error) {
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
	conf := &TLSConfig{
		DBName: dbName,
		Config: &tls.Config{
			RootCAs:      rootCertPool,
			Certificates: clientCert,
			ServerName:   serverName,
		},
	}
	return conf, nil
}
