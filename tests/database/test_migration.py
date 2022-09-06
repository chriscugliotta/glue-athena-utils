import pytest
from aws_utils.database.migration import DatabaseMigrationService


@pytest.mark.parametrize('db_id', ['sqlite', 'glue'])
def test_upgrade_downgrade(databases, db_id):
    db = databases[db_id]
    dms = DatabaseMigrationService(
        db=db,
        script_path='tests.data.resources.migrations',
        script_args={'env': 'dev'},
        version_table_name='test_version',
        version_table_path=f'{db.s3_database_prefix}/test_version/',
    )
    dms.migrate(3)
    dms.migrate(0, downgrade=True)
