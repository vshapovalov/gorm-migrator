package migrator

import (
	"github.com/stretchr/testify/require"
	"github.com/vshapovalov/gorm-migrator/mocks"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"
)

const testMigrationTable = "migrations_table"

func Test_Migrator_RunCheck(t *testing.T) {

	loggerMock := new(mocks.ILogger)

	migrations := createTestMigrations()

	sqlMock, dbClient, err := createDbClient()
	require.NoError(t, err)
	expectCreateTable(sqlMock, testMigrationTable)
	migrator, err := createMigrator(migrations, loggerMock, dbClient, testMigrationTable)
	require.NoError(t, err)

	// mark migrations 0 and 2 as executed
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations[0], migrations[2])

	// expecting list of migrations that would be executed - 1,3,4
	expectSuccessCheck(loggerMock, migrations[1], migrations[3], migrations[4])

	err = migrator.RunCheck()
	require.NoError(t, err)

	loggerMock.AssertExpectations(t)
	require.NoError(t, sqlMock.ExpectationsWereMet())
}

func Test_Migrator_Run(t *testing.T) {

	loggerMock := new(mocks.ILogger)

	migrations := createTestMigrations()

	sqlMock, dbClient, err := createDbClient()
	require.NoError(t, err)
	expectCreateTable(sqlMock, testMigrationTable)
	migrator, err := createMigrator(migrations, loggerMock, dbClient, testMigrationTable)
	require.NoError(t, err)

	// mark migrations 0 and 2 as executed
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations[0], migrations[2], migrations[4])

	// expecting list of migrations that would be executed - 1,3,4
	expectSuccessExecute(sqlMock, loggerMock, testMigrationTable, migrations[1], migrations[3])

	err = migrator.Run()
	require.NoError(t, err)

	loggerMock.AssertExpectations(t)
	require.NoError(t, sqlMock.ExpectationsWereMet())
}

func Test_Migrator_RunStepCheck(t *testing.T) {

	loggerMock := new(mocks.ILogger)

	migrations := createTestMigrations()
	sqlMock, dbClient, err := createDbClient()
	require.NoError(t, err)
	expectCreateTable(sqlMock, testMigrationTable)
	migrator, err := createMigrator(migrations, loggerMock, dbClient, testMigrationTable)
	require.NoError(t, err)

	// mark migrations 0 and 2 as executed
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations[0], migrations[2])

	// expecting list of 2 migrations that would be executed - 1,3
	expectSuccessCheck(loggerMock, migrations[1], migrations[3])

	err = migrator.RunStepCheck(2)
	require.NoError(t, err)

	loggerMock.AssertExpectations(t)
	require.NoError(t, sqlMock.ExpectationsWereMet())
}

func Test_Migrator_RunStep(t *testing.T) {

	loggerMock := new(mocks.ILogger)

	migrations := createTestMigrations()
	sqlMock, dbClient, err := createDbClient()
	require.NoError(t, err)
	expectCreateTable(sqlMock, testMigrationTable)
	migrator, err := createMigrator(migrations, loggerMock, dbClient, testMigrationTable)
	require.NoError(t, err)

	// mark migrations 0 and 2 as executed
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations[0], migrations[2])

	// expecting list of 2 migrations that would be executed - 1,3
	expectSuccessExecute(sqlMock, loggerMock, testMigrationTable, migrations[1], migrations[3])

	err = migrator.RunStep(2)
	require.NoError(t, err)

	loggerMock.AssertExpectations(t)
	require.NoError(t, sqlMock.ExpectationsWereMet())
}

func Test_Migrator_RollbackStepCheck(t *testing.T) {

	loggerMock := new(mocks.ILogger)

	migrations := createTestMigrations()
	sqlMock, dbClient, err := createDbClient()
	require.NoError(t, err)
	expectCreateTable(sqlMock, testMigrationTable)
	migrator, err := createMigrator(migrations, loggerMock, dbClient, testMigrationTable)
	require.NoError(t, err)

	// no migrations rollback
	exceptExecutedMigrations(sqlMock, testMigrationTable)
	loggerMock.On("Info", noAvailableMigrations)
	err = migrator.RollbackStepCheck(2)
	require.NoError(t, err)

	// last migration rolled back
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations...)
	//	show last 2 executed migrations
	expectRollbackCheck(loggerMock, migrations[4])
	err = migrator.RollbackStepCheck(1)
	require.NoError(t, err)

	// all migrations rolled back
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations...)
	expectRollbackCheck(loggerMock, migrations...)
	err = migrator.RollbackStepCheck(len(migrations))
	require.NoError(t, err)

	// no migrations rolled back
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations...)
	expectRollbackCheck(loggerMock)
	err = migrator.RollbackStepCheck(0)
	require.NoError(t, err)

	// 3 migrations rolled back
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations[1], migrations[2], migrations[4])
	expectRollbackCheck(loggerMock, migrations[4], migrations[2], migrations[1])
	err = migrator.RollbackStepCheck(3)
	require.NoError(t, err)

	// 2 migrations rolled back
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations[1], migrations[2], migrations[4])
	expectRollbackCheck(loggerMock, migrations[4], migrations[2])
	err = migrator.RollbackStepCheck(2)
	require.NoError(t, err)

	loggerMock.AssertExpectations(t)
	require.NoError(t, sqlMock.ExpectationsWereMet())
}

func Test_Migrator_RollbackStep(t *testing.T) {

	loggerMock := new(mocks.ILogger)

	migrations := createTestMigrations()
	sqlMock, dbClient, err := createDbClient()
	require.NoError(t, err)
	expectCreateTable(sqlMock, testMigrationTable)
	migrator, err := createMigrator(migrations, loggerMock, dbClient, testMigrationTable)
	require.NoError(t, err)

	// no migrations rollback
	exceptExecutedMigrations(sqlMock, testMigrationTable)
	loggerMock.On("Info", noAvailableMigrations)
	err = migrator.RollbackStep(2)
	require.NoError(t, err)

	// last migration rolled back
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations...)
	//	show last 2 executed migrations
	expectSuccessRollback(sqlMock, loggerMock, testMigrationTable, migrations[4])
	err = migrator.RollbackStep(1)
	require.NoError(t, err)

	// all migrations rolled back
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations...)
	expectSuccessRollback(sqlMock, loggerMock, testMigrationTable, migrations[4], migrations[3], migrations[2], migrations[1], migrations[0])
	err = migrator.RollbackStep(len(migrations))
	require.NoError(t, err)

	// no migrations rolled back
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations...)
	expectSuccessRollback(sqlMock, loggerMock, testMigrationTable)
	err = migrator.RollbackStep(0)
	require.NoError(t, err)

	// 3 migrations rolled back
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations[1], migrations[2], migrations[4])
	expectSuccessRollback(sqlMock, loggerMock, testMigrationTable, migrations[4], migrations[2], migrations[1])
	err = migrator.RollbackStep(3)
	require.NoError(t, err)

	// 2 migrations rolled back
	exceptExecutedMigrations(sqlMock, testMigrationTable, migrations[1], migrations[2], migrations[4])
	expectSuccessRollback(sqlMock, loggerMock, testMigrationTable, migrations[4], migrations[2])
	err = migrator.RollbackStep(2)
	require.NoError(t, err)

	loggerMock.AssertExpectations(t)
	require.NoError(t, sqlMock.ExpectationsWereMet())
}

func createDbClient() (sqlmock.Sqlmock, *gorm.DB, error) {
	db, sqlMock, err := sqlmock.New()
	if err != nil {
		return nil, nil, err
	}

	gormClient, err := gorm.Open(
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
	return sqlMock, gormClient, err
}

func createMigrator(
	migrations []Migration,
	loggerMock ILogger,
	dbClient *gorm.DB,
	migrationTable string,
) (IMigrator, error) {
	migrator, err := NewMigrator(migrations, Config{Db: dbClient, Table: migrationTable, Logger: loggerMock})
	return migrator, err
}

func expectCreateTable(mock sqlmock.Sqlmock, migrationTable string) {
	mock.
		ExpectExec(regexp.QuoteMeta("CREATE TABLE `" + migrationTable + "` (`id` smallint unsigned AUTO_INCREMENT NOT NULL,`migration` varchar(191) NOT NULL,PRIMARY KEY (`id`))")).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectSuccessCheck(loggerMock *mocks.ILogger, migrations ...Migration) {
	for _, migration := range migrations {
		loggerMock.On("Info", migrationExecuted, "id", migration.Id)
	}
}

func expectRollbackCheck(loggerMock *mocks.ILogger, migrations ...Migration) {
	for _, migration := range migrations {
		loggerMock.On("Info", migrationRolledBack, "id", migration.Id)
	}
}

func exceptExecutedMigrations(mock sqlmock.Sqlmock, migrationTable string, migrations ...Migration) {
	rows := sqlmock.NewRows([]string{"migration"})
	for _, migration := range migrations {
		rows.AddRow(migration.Id)
	}
	mock.
		ExpectQuery(regexp.QuoteMeta("SELECT migration FROM `" + migrationTable + "` ORDER BY id asc")).
		WillReturnRows(rows)
}

func expectSuccessExecute(
	mock sqlmock.Sqlmock,
	logger *mocks.ILogger,
	migrationTable string,
	migrations ...Migration,
) {
	for _, migration := range migrations {
		mock.ExpectBegin()
		mock.
			ExpectExec(regexp.QuoteMeta("execute " + migration.Id)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.
			ExpectExec(regexp.QuoteMeta("insert into " + migrationTable + " (migration) values (?)")).
			WithArgs(migration.Id).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit()
		logger.On("Info", migrationExecuted, "id", migration.Id)
	}
}

func expectSuccessRollback(
	mock sqlmock.Sqlmock,
	logger *mocks.ILogger,
	migrationTable string,
	migrations ...Migration,
) {
	for _, migration := range migrations {
		mock.ExpectBegin()
		mock.
			ExpectExec(regexp.QuoteMeta("rollback " + migration.Id)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.
			ExpectExec(regexp.QuoteMeta("delete from `" + migrationTable + "` where `migration` = ?")).
			WithArgs(migration.Id).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit()
		logger.On("Info", migrationRolledBack, "id", migration.Id)
	}
}

func createTestMigrations() []Migration {
	var migrations []Migration
	for i := 0; i < 5; i++ {
		migrations = append(migrations, Migration{
			Id: "migration_" + strconv.Itoa(i),
			Migrate: func(migrationId int) func(tx *gorm.DB) error {
				return func(tx *gorm.DB) error {
					return tx.Exec("execute migration_" + strconv.Itoa(migrationId)).Error
				}
			}(i),
			Rollback: func(migrationId int) func(tx *gorm.DB) error {
				return func(tx *gorm.DB) error {
					return tx.Exec("rollback migration_" + strconv.Itoa(migrationId)).Error
				}
			}(i),
		})
	}
	return migrations
}
