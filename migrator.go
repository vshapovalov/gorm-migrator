package migrator

import (
	"gorm.io/gorm"
)

const (
	defaultMigrationTableName = "migrations"

	actionMigrate = iota
	actionRollback

	noAvailableMigrations = "no available migrations"
	migrationRolledBack   = "migration rolled back"
	migrationFailed       = "migration failed"
	migrationExecuted     = "migration executed"
)

// IMigrator manages migrations in the project
type IMigrator interface {
	// Run execute all new migrations from current.
	// Skip migrations that have been executed.
	Run() error
	// RunCheck show all new migrations from current.
	// Skip migrations that have been executed.
	RunCheck() error
	// RunStep execute specified quantity of new migrations from current.
	// For example, we have three new migrations, RunStep(2) will execute only first two of them from current.
	RunStep(step int) error
	// RunStepCheck show list of migrations that wil be executed.
	// For example, we have three new migrations, RunStepCheck(2) will show only first two of them from current.
	RunStepCheck(step int) error
	// RollbackStep execute specified quantity of migrations that wil be rolled back from current.
	// For example, we have three executed migrations, RollbackStep(2) will roll back only two of them from current.
	RollbackStep(step int) error
	// RollbackStepCheck show list of migrations that wil be rolled back from current.
	// For example, we have three executed migrations, RollbackStepCheck(2) will show only two of them from current.
	RollbackStepCheck(step int) error
}

// Resolver provides a list of executed migrations
type Resolver func(db *gorm.DB) []string

// Config migrator configuration
type Config struct {
	// Db gorm db client
	Db *gorm.DB
	// Table where the list of executed migrations is stored
	Table string
	// default migration resolver is used, if nil
	Logger ILogger
}

type Migrator struct {
	migrations     []Migration
	config         Config
	executedCount  int
	availableCount int
}

func NewMigrator(migrations []Migration, config Config) (*Migrator, error) {
	if config.Logger == nil {
		config.Logger = NewStdoutLogger(true)
	}
	m := Migrator{
		migrations: migrations,
		config:     config,
	}

	if m.config.Table == "" {
		m.config.Table = defaultMigrationTableName
	}

	err := m.createMigrationTable()
	if err != nil {
		return nil, err
	}

	return &m, nil
}

// create migration table in database
func (m *Migrator) createMigrationTable() error {
	return m.config.Db.Exec(
		"CREATE TABLE IF NOT EXISTS `" + m.config.Table + "` (" +
			"  `id` INT (10) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"  `migration` VARCHAR (191) NOT NULL," +
			"  PRIMARY KEY (`id`)" +
			") ENGINE = INNODB",
	).Error
}

// get list of executed migrations from migrations repository
func (m *Migrator) getExecutedMigrationList() ([]string, error) {
	var list []string

	err := m.config.Db.
		Table(m.config.Table).
		Select("migration").
		Order("id asc").
		Scan(&list).Error

	return list, err
}

// mark migration as executed by adding it to migrations repository
func (m *Migrator) markMigrationExecuted(id string, tx *gorm.DB) error {
	return tx.Exec("insert into `"+m.config.Table+"` (`migration`) values (?)", id).Error
}

// remove migration from executed list - remove it from migrations repository
func (m *Migrator) removeMigrationExecutedMark(id string, tx *gorm.DB) error {
	return tx.Exec("delete from `"+m.config.Table+"` where `migration` = ?", id).Error
}

// execute specified migration handlers in transaction
func (m *Migrator) executeMigration(migration Migration, action int) error {
	err := m.config.Db.Transaction(func(tx *gorm.DB) error {
		if action == actionMigrate {
			err := migration.Migrate(tx)
			if err != nil {
				return err
			}
			return m.markMigrationExecuted(migration.Id, tx)
		}
		err := migration.Rollback(tx)
		if err != nil {
			return err
		}
		return m.removeMigrationExecutedMark(migration.Id, tx)
	})
	return err
}

// get migrations that have not been executed yet
func (m *Migrator) getMigrationsForRun() ([]Migration, error) {
	executed, err := m.getExecutedMigrationList()
	if err != nil {
		return nil, err
	}

	var res []Migration
	if executed == nil {
		for _, migration := range m.migrations {
			res = append(res, migration)
		}
	} else {
		for _, migration := range m.migrations {
			if Contains(executed, migration.Id) {
				continue
			}
			res = append(res, migration)
		}
	}

	return res, nil
}

// get migrations that have been executed already
func (m *Migrator) getMigrationsForRollback() ([]Migration, error) {
	executed, err := m.getExecutedMigrationList()
	if err != nil {
		return nil, err
	}

	var res []Migration
	if executed != nil {
		for _, migration := range m.migrations {
			if Contains(executed, migration.Id) {
				res = append(res, migration)
			}
		}
	}

	return res, nil
}

// invoke runner for list of migrations
func (m *Migrator) runMigrationList(list []Migration, count int, runner func(Migration) error) error {
	for i := 0; i < len(list) && i < count; i++ {
		err := runner(list[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) rollbackMigrationList(list []Migration, count int, runner func(Migration) error) error {
	lenList := len(list)

	if count > lenList {
		count = lenList
	}

	for i := 0; i < count; i++ {
		err := runner(list[lenList-1-i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) Run() error {
	forRun, err := m.getMigrationsForRun()
	if err != nil {
		return err
	}
	if len(forRun) == 0 {
		m.config.Logger.Info(noAvailableMigrations)
		return nil
	}
	err = m.runMigrationList(forRun, len(forRun), func(migration Migration) error {
		err := m.executeMigration(migration, actionMigrate)
		if err != nil {
			m.config.Logger.Info(migrationFailed, "id", migration.Id, "err", err)
			return err
		}
		m.config.Logger.Info(migrationExecuted, "id", migration.Id)
		return nil
	})
	return err
}

func (m *Migrator) RunCheck() error {
	forRun, err := m.getMigrationsForRun()
	if err != nil {
		return err
	}
	if len(forRun) == 0 {
		m.config.Logger.Info(noAvailableMigrations)
		return nil
	}
	err = m.runMigrationList(forRun, len(forRun), func(migration Migration) error {
		m.config.Logger.Info(migrationExecuted, "id", migration.Id)
		return nil
	})
	return err
}

func (m *Migrator) RunStep(step int) error {
	forRun, err := m.getMigrationsForRun()
	if err != nil {
		return err
	}
	if len(forRun) == 0 {
		m.config.Logger.Info(noAvailableMigrations)
		return nil
	}
	err = m.runMigrationList(forRun, step, func(migration Migration) error {
		err := m.executeMigration(migration, actionMigrate)
		if err != nil {
			m.config.Logger.Info(migrationFailed, "id", migration.Id, "err", err)
			return err
		}
		m.config.Logger.Info(migrationExecuted, "id", migration.Id)
		return nil
	})
	return err
}

func (m *Migrator) RunStepCheck(step int) error {
	forRun, err := m.getMigrationsForRun()
	if err != nil {
		return err
	}
	if len(forRun) == 0 {
		m.config.Logger.Info(noAvailableMigrations)
		return nil
	}
	err = m.runMigrationList(forRun, step, func(migration Migration) error {
		m.config.Logger.Info(migrationExecuted, "id", migration.Id)
		return nil
	})
	return err
}

func (m *Migrator) RollbackStep(step int) error {
	forRollback, err := m.getMigrationsForRollback()
	if err != nil {
		return err
	}
	if len(forRollback) == 0 {
		m.config.Logger.Info(noAvailableMigrations)
		return nil
	}
	err = m.rollbackMigrationList(forRollback, step, func(migration Migration) error {
		err := m.executeMigration(migration, actionRollback)
		if err != nil {
			m.config.Logger.Info(migrationFailed, "id", migration.Id, "err", err)
			return err
		}
		m.config.Logger.Info(migrationRolledBack, "id", migration.Id)
		return nil
	})
	return err
}

func (m *Migrator) RollbackStepCheck(step int) error {
	forRollback, err := m.getMigrationsForRollback()
	if err != nil {
		return err
	}
	if len(forRollback) == 0 {
		m.config.Logger.Info(noAvailableMigrations)
		return nil
	}
	if step == 0 {
		return nil
	}
	err = m.rollbackMigrationList(forRollback, step, func(migration Migration) error {
		m.config.Logger.Info(migrationRolledBack, "id", migration.Id)
		return nil
	})
	return err
}
