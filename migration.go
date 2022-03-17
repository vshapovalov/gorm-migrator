package migrator

import (
	"gorm.io/gorm"
	"io/ioutil"
)

type MigrationHandler func(tx *gorm.DB) error

// Migration contains handlers that will be run by migrator
// Must have unique id
type Migration struct {
	Id       string
	Migrate  MigrationHandler
	Rollback MigrationHandler
}

// NewFileMigration Create migration from files
// Read files and run raw sql
func NewFileMigration(id, migrateFile, rollbackFile string) Migration {
	return Migration{
		Id:       id,
		Migrate:  makeHandlerFromFile(migrateFile),
		Rollback: makeHandlerFromFile(rollbackFile),
	}
}

// Returns handle that read file and run raw sql
func makeHandlerFromFile(file string) MigrationHandler {
	return func(tx *gorm.DB) error {
		readFile, err := ioutil.ReadFile(file)
		if err != nil {
			return err
		}
		return tx.Exec(string(readFile)).Error
	}
}
