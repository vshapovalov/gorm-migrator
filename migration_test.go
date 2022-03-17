package migrator

import (
	"database/sql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"testing"
	"time"
)

type SuiteMigration struct {
	suite.Suite
	DB   *gorm.DB
	mock sqlmock.Sqlmock

	licenseSignSecret string
}

func (s *SuiteMigration) SetupSuite() {
	var (
		db  *sql.DB
		err error
	)

	db, s.mock, err = sqlmock.New()
	require.NoError(s.T(), err)

	s.DB, err = gorm.Open(
		mysql.New(mysql.Config{Conn: db, SkipInitializeWithVersion: true}),
		&gorm.Config{
			DisableAutomaticPing: true,
			Logger: logger.New(
				log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
				logger.Config{
					SlowThreshold:             time.Second,   // Slow SQL threshold
					LogLevel:                  logger.Silent, // Log level
					Colorful:                  true,          // Disable color
					IgnoreRecordNotFoundError: true,
				},
			),
		},
	)
	require.NoError(s.T(), err)
}

func (s *SuiteMigration) AfterTest(_, _ string) {
	require.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func TestMigrationInit(t *testing.T) {
	suite.Run(t, new(SuiteMigration))
}

// check that method use correct sql query
func (s *SuiteMigration) Test_NewFileMigration() {

	migrateFileContent := "create table users (`ololo` int);"
	migrateFile := "migrate.sql"

	rollbackFileContent := "drop table users;"
	rollbackFile := "rollback.sql"

	migrationId := "create_users_table"

	err := ioutil.WriteFile(migrateFile, []byte(migrateFileContent), 0666)
	require.NoError(s.T(), err)
	err = ioutil.WriteFile(rollbackFile, []byte(rollbackFileContent), 0666)
	require.NoError(s.T(), err)

	migration := NewFileMigration(migrationId, migrateFile, rollbackFile)

	s.mock.ExpectExec(regexp.QuoteMeta(migrateFileContent)).WithArgs().WillReturnResult(sqlmock.NewResult(0, 0))
	s.mock.ExpectExec(regexp.QuoteMeta(rollbackFileContent)).WithArgs().WillReturnResult(sqlmock.NewResult(0, 0))

	err = migration.Migrate(s.DB)
	require.NoError(s.T(), err)

	err = migration.Rollback(s.DB)
	require.NoError(s.T(), err)

	require.Equal(s.T(), migrationId, migration.Id)
}
