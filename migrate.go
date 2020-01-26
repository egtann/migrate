package migrate

import (
	"bytes"
	"crypto/md5"
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

// version of the migrate tool's database schema.
const version = 1

type Migrate struct {
	Migrations []Migration
	Files      []os.FileInfo

	db  Store
	log Logger
	dir string
	idx int
}

type Migration struct {
	Filename string
	Checksum string
	Content  string
}

var regexNum = regexp.MustCompile(`^\d+`)

func New(
	db Store,
	log Logger,
	dir, skip string,
) (*Migrate, error) {
	m := &Migrate{db: db, log: log, dir: dir}

	// Get files in migration dir and sort them
	var err error
	m.Files, err = readdir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "get migrations")
	}
	if err = sortfiles(m.Files); err != nil {
		return nil, errors.Wrap(err, "sort")
	}

	// Create meta tables if we need to, so we can store the migration
	// state in the db itself
	if err = db.CreateMetaIfNotExists(); err != nil {
		return nil, errors.Wrap(err, "create meta table")
	}
	if err = db.CreateMetaCheckpointsIfNotExists(); err != nil {
		return nil, errors.Wrap(err, "create meta checkpoints table")
	}
	curVersion, err := db.CreateMetaVersionIfNotExists()
	if err != nil {
		return nil, errors.Wrap(err, "create meta version table")
	}

	// Migrate the database schema to match the tool's expectations
	// automatically
	if curVersion > version {
		return nil, errors.New("must upgrade migrate: go get -u github.com/egtann/migrate")
	}
	if curVersion < 1 {
		tmpMigrations, err := migrationsFromFiles(m)
		if err != nil {
			return nil, errors.Wrap(err, "migrations from files")
		}
		if err = db.UpgradeToV1(tmpMigrations); err != nil {
			return nil, errors.Wrap(err, "upgrade to v1")
		}
	}

	// If skip, then we record the migrations but do not perform them. This
	// enables you to start using this package on an existing database
	if skip != "" {
		m.idx, err = m.skip(skip)
		if err != nil {
			return nil, errors.Wrap(err, "skip ahead")
		}
		m.log.Println("skipped ahead")
	}

	// Get all migrations
	m.Migrations, err = db.GetMigrations()
	if err != nil {
		return nil, errors.Wrap(err, "get migrations")
	}

	if err = m.validHistory(); err != nil {
		return nil, err
	}
	return m, nil
}

// Migrate all files in the directory. This function reports whether any
// migration took place.
func (m *Migrate) Migrate() (bool, error) {
	var migrated bool
	for i := len(m.Migrations); i < len(m.Files); i++ {
		filename := m.Files[i].Name()
		if err := m.migrateFile(filename); err != nil {
			return false, errors.Wrap(err, "migrate file")
		}
		m.log.Println("migrated", filename)
		migrated = true
	}
	return migrated, nil
}

func (m *Migrate) validHistory() error {
	for i := len(m.Files); i < len(m.Migrations); i++ {
		m.log.Printf("missing already-run migration %q\n", m.Migrations[i])
	}
	if len(m.Files) < len(m.Migrations) {
		return errors.New("cannot continue with missing migrations")
	}
	for i := m.idx; i < len(m.Migrations); i++ {
		mg := m.Migrations[i]
		if mg.Filename != m.Files[i].Name() {
			m.log.Printf("\n%s was added to history before %s.",
				m.Files[i].Name(), mg.Filename)
			return errors.New("failed to migrate. migrations must be appended")
		}
		if err := m.checkHash(mg); err != nil {
			return errors.Wrap(err, "check hash")
		}
	}
	return nil
}

func (m *Migrate) checkHash(mg Migration) error {
	fi, err := os.Open(filepath.Join(m.dir, mg.Filename))
	if err != nil {
		return err
	}
	defer fi.Close()
	_, check, err := computeChecksum(fi)
	if err != nil {
		return err
	}
	if check != mg.Checksum {
		m.log.Println("comparing", check, mg.Checksum)
		return fmt.Errorf("checksum does not match %s. has the file changed?",
			mg.Filename)
	}
	return nil
}

func (m *Migrate) migrateFile(filename string) error {
	pth := filepath.Join(m.dir, filename)
	byt, err := ioutil.ReadFile(pth)
	if err != nil {
		return err
	}

	// Split commands and remove comments at the start of lines
	cmds := strings.Split(string(byt), ";")
	filteredCmds := []string{}
	for _, cmd := range cmds {
		cmd = strings.TrimSpace(cmd)
		if len(cmd) > 0 && !strings.HasPrefix(cmd, "--") {
			filteredCmds = append(filteredCmds, cmd)
		}
	}

	// Ensure that commands are present
	if len(filteredCmds) == 0 {
		return fmt.Errorf("no sql statements in file: %s", filename)
	}

	// Get our checkpoints, if any
	checkpoints, err := m.db.GetMetaCheckpoints(filename)
	if err != nil {
		return errors.Wrap(err, "get checkpoints")
	}
	if len(checkpoints) > 0 {
		m.log.Printf("found %d checkpoints\n", len(checkpoints))
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
			_, checksum, err := computeChecksum(r)
			if err != nil {
				return errors.Wrap(err, "compute checkpoint checksum")
			}
			if checksum != checkpoints[i] {
				return fmt.Errorf(
					"checksum does not equal checkpoint. has %s (cmd %d) changed?",
					filename, i)
			}
			continue
		}

		// Execute non-checkpointed commands one by one
		_, err := m.db.Exec(cmd)
		if err != nil {
			m.log.Println("failed on", cmd)
			return fmt.Errorf("%s: %s", filename, err)
		}

		// Save a checkpoint
		_, checksum, err := computeChecksum(strings.NewReader(cmd))
		if err != nil {
			return errors.Wrap(err, "compute checksum")
		}
		err = m.db.InsertMetaCheckpoint(filename, cmd, checksum, i)
		if err != nil {
			return errors.Wrap(err, "insert checkpoint")
		}
	}

	// We've successfully finished migrating the file, so we delete the
	// temporary progress in metacheckpoints and save the migration
	if err = m.db.DeleteMetaCheckpoints(); err != nil {
		return errors.Wrap(err, "delete checkpoints")
	}

	_, checksum, err := computeChecksum(bytes.NewReader(byt))
	if err != nil {
		return errors.Wrap(err, "compute file checksum")
	}
	if err = m.db.InsertMigration(filename, string(byt), checksum); err != nil {
		return errors.Wrap(err, "insert migration")
	}
	return nil
}

func (m *Migrate) skip(toFile string) (int, error) {
	// Get just the filename if skip is a directory
	_, toFile = filepath.Split(toFile)

	// Ensure the file exists
	index := -1
	for i, fi := range m.Files {
		if fi.Name() == toFile {
			index = i
			break
		}
	}
	if index == -1 {
		return 0, fmt.Errorf("%s does not exist", toFile)
	}
	for i := 0; i <= index; i++ {
		name := m.Files[i].Name()
		fi, err := os.Open(filepath.Join(m.dir, name))
		if err != nil {
			return -1, err
		}
		content, checksum, err := computeChecksum(fi)
		if err != nil {
			fi.Close()
			return -1, err
		}
		if err = m.db.UpsertMigration(name, content, checksum); err != nil {
			fi.Close()
			return -1, err
		}
		if err = fi.Close(); err != nil {
			return -1, err
		}
	}
	return index, nil
}

func computeChecksum(r io.Reader) (content string, checksum string, err error) {
	h := md5.New()
	byt, err := ioutil.ReadAll(r)
	if err != nil {
		return "", "", errors.Wrap(err, "read all")
	}
	if _, err := io.Copy(h, bytes.NewReader(byt)); err != nil {
		return "", "", err
	}
	return string(byt), fmt.Sprintf("%x", h.Sum(nil)), nil
}

// readdir collects file infos from the migration directory.
func readdir(dir string) ([]os.FileInfo, error) {
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

// sortfiles by name, ensuring that something like 1.sql, 2.sql, 10.sql is
// ordered correctly.
func sortfiles(files []os.FileInfo) error {
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

func migrationsFromFiles(m *Migrate) ([]Migration, error) {
	ms := make([]Migration, len(m.Files))
	for i, fileInfo := range m.Files {
		filename := filepath.Join(m.dir, fileInfo.Name())
		byt, err := ioutil.ReadFile(filename)
		if err != nil {
			return nil, errors.Wrap(err, "read file")
		}
		ms[i] = Migration{
			Filename: fileInfo.Name(),
			Content:  string(byt),
		}
	}
	return ms, nil
}
