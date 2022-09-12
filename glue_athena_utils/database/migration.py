import importlib
import logging
import time
from contextlib import contextmanager
from pandas import DataFrame
from pathlib import Path
log = logging.getLogger(__name__)



class DatabaseMigrationService:
    """
    A service that ensures database schema is in sync with application.

    This service uses migration scripts to systematically and automatically apply schema changes
    to a living database.  It will create and maintain a `version` table, which always contains the
    database's current version.  Prior to running, this service assumes the database is initially
    empty, thus the `version` table is always initialized at `version = 0`.

    When an application needs to upgrade its database, it can call this service (via the `migrate`
    function), and pass in a _target_ version.  For example, suppose the database's current version
    is 1, and target version is 3.  In this case, two migration scripts will be called:  first the
    v1-to-v2 upgrade script, then the v2-to-v3 upgrade script.  Downgrades are also possible.

    Migration scripts can be written via arbitrary Python code.  There are only two requirements:
    First, each migration script must have an `upgrade` and `downgrade` function.  Second, the
    migration scripts must obey the following directory and file naming pattern:

        .
        ├── v1
        │   └── upgrade.py
        ├── v2
        │   └── upgrade.py
        └── v3
            └── upgrade.py

    See the unit test (at `tests/database/test_migration.py`) for an example implementation.

    Note:
        Conceptually, this whole thing behaves like Liquibase.  Why reinvent the wheel?  Because
        Liquibase has 10,000 pages of documentation, whereas this class gets the job done in <100
        lines of Python code.  This simplified implementation is sufficient for many use cases.

    Attributes:
        db (DatabaseConnection):
            Database interface.

        script_path (str):
            Python path from which migration scripts can be imported.
            For example, suppose our repo is structured like this:

                path/to/migrations/v1/upgrade.py
                path/to/migrations/v2/upgrade.py

            Then this argument should be `path.to.migrations`.

        script_args (dict):
            Within each migration script, the `upgrade` and `downgrade` functions will be passed
            these arguments (as keyword arguments).  This can be used to inject application-specific
            variables into the migration logic, such as deployment environment or config info.

        version_table_name (str):
            Name of `version` table.

        version_table_path (str):
            S3 path of `version` table.  (Only required for Glue databases.)
    """

    def __init__(self, db, script_path, script_args={}, version_table_name='version', version_table_path=None):
        self.db = db
        self.script_path = script_path
        self.script_args = script_args
        self.version_table_name = version_table_name
        self.version_table_path = version_table_path
        log.info(f'Constructed new DatabaseMigrationService!  db = {self.db}, version_table_name = {self.version_table_name}')

    def migrate(self, target_version, downgrade=False):
        """Upgrades (or downgrades) database schema to target version."""

        # Log.
        log.info(f'Checking if database migrations are needed at:  {self.db.name}.')

        # If database is locked, wait.
        current_version = self._wait_until_unlocked(target_version)

        # Are any migrations needed?
        needed = (
            (current_version < target_version) or
            (current_version > target_version and downgrade)
        )

        # Lock, apply migrations, unlock.
        if needed:
            with self._lock(current_version, target_version):
                self._apply_migrations(current_version, target_version, downgrade)
        else:
            log.info('Database is up-to-date.')

    def _wait_until_unlocked(self, target_version, max_attempts=5*4, delay=15):

        for i in range(max_attempts):

            # Check the version table.
            current_version, locked = self._get_current_version()
            log.info(f'locked = {locked}, current_version = {current_version}, target_version = {target_version}.')

            # Check timeout.
            if i >= max_attempts - 1:
                raise Exception('Timed out while waiting for database to unlock.')

            # If locked, wait a bit, then check again.
            if locked:
                time.sleep(delay)
            else:
                return current_version

    def _get_current_version(self):
        try:
            df = self.db.select(sql=f'select * from {self.version_table_name}', quiet=True)
            return int(df['version'].iloc[0]), int(df['locked'].iloc[0])
        except Exception as e:
            if 'no such table' in e.args[0] or 'does not exist' in e.args[0]:
                log.debug('Version table does not exist.')
                self._create_version_table()
                return self._get_current_version()

    def _create_version_table(self):
        self.db.execute(sql=self._get_version_table_ddl())
        self.db.execute(sql=f'insert into {self.version_table_name} values (0, 0)')

    def _get_version_table_ddl(self):
        if self.db.type == 'glue':
            return f"create external table {self.version_table_name} (version int, locked int) location '{self.version_table_path}'"
        else:
            return f"create table {self.version_table_name} (version int, locked int)"

    @contextmanager
    def _lock(self, current_version, target_version):
        self._update(version=current_version, locked=True)
        yield
        self._update(version=target_version, locked=False)

    def _update(self, version, locked):
        """Athena doesn't support SQL update statements."""
        if self.db.type == 'glue':
            df = DataFrame([{'version': version, 'locked': int(locked)}])
            self.db.insert(df=df, table_name=self.version_table_name, mode='overwrite')
        else:
            self.db.execute(sql=f"update {self.version_table_name} set version = {version}, locked = {int(locked)}")

    def _apply_migrations(self, current_version, target_version, downgrade):

        # Are we upgrading, downgrading, or up-to-date?
        if current_version < target_version:
            func = 'upgrade'
            start = current_version + 1
            stop = target_version + 1
            step = 1
        elif current_version > target_version:
            func = 'downgrade'
            start = current_version
            stop = target_version
            step = -1
            if not downgrade: return
        else:
            return

        # Dynamically import and run migration script(s).
        for version in range(start, stop, step):
            module_path = '{}.v{}.upgrade'.format(self.script_path, version)
            module = importlib.import_module(module_path)
            module_dir = Path(module.__file__).absolute().parent
            log.info(f'Begin migrating database from version {version - step - downgrade} to {version - downgrade}.')
            getattr(module, func)(self.db, module_dir, **self.script_args)
            log.info(f'Done migrating database from version {version - step - downgrade} to {version - downgrade}.')
