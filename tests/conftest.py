import boto3
import logging
import logging.config
import pytest
import sys
from pathlib import Path

# Hack Python path.
path_repo = Path(__file__).absolute().parents[1]
if str(path_repo) not in sys.path:
    sys.path.insert(0, str(path_repo))

from glue_athena_utils.database.connection import DatabaseConnection
from glue_athena_utils.s3.connection import S3Connection
from tests.config import config
log = logging.getLogger('cc_utils.conftest')





@pytest.fixture(scope='session', autouse=True)
def log_file(worker_id):
    log_path = config.paths.logs / 'log{}.log'.format(_get_worker_suffix(worker_id))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    config.log['handlers']['file']['filename'] = log_path
    logging.config.dictConfig(config.log)
    log.info('worker_id = {0}'.format(worker_id))


@pytest.fixture(scope='session')
def boto3_session():
    return boto3.session.Session(profile_name=config.aws_profile)


@pytest.fixture(scope='session')
def s3():
    return S3Connection(config.s3_bucket, config.aws_profile, config.aws_proxy_url)


@pytest.fixture(scope='session')
def sqlite_db(worker_id):
    path = config.paths.mock_databases / 'test_sqlite{}.db'.format(_get_worker_suffix(worker_id))
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        path.unlink()
    return DatabaseConnection(type='sqlite', name=path.name, url=path)


@pytest.fixture(scope='session')
def glue_db(boto3_session):
    return DatabaseConnection(**_get_db_config('glue'), boto3_session=boto3_session)


@pytest.fixture(scope='session')
def redshift_db():
    return DatabaseConnection(**_get_db_config('redshift'))


@pytest.fixture(scope='session')
def oracle_db():
    return DatabaseConnection(**_get_db_config('oracle'))


@pytest.fixture(scope='session')
def mysql_db():
    return DatabaseConnection(**_get_db_config('mysql'))


@pytest.fixture(scope='session')
def databases(sqlite_db, oracle_db, glue_db, redshift_db, mysql_db):
    return {
        'sqlite': sqlite_db,
        'glue': glue_db,
        'redshift': redshift_db,
        'oracle': oracle_db,
        'mysql': mysql_db,
    }


def _get_db_config(db_id):
    db_config = dict(config.databases[db_id])
    db_config['type'] = db_id
    return db_config


def _get_worker_suffix(worker_id):
    return '' if worker_id == 'master' else '_' + worker_id[-1]
