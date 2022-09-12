from pathlib import Path



class Config:
    """Test configuration."""

    def __init__(self):
        self.aws_profile = 'default'
        self.aws_proxy_url = None
        self.databases = self._get_databases()
        self.log = self._get_log()
        self.paths = ConfigPaths()
        self.s3_bucket = 'bucket-name'

    def _get_databases(self):
        return {
            'glue': {
                'name': 'secret',
                's3_database_prefix': 's3://{bucket}/{prefix}',
                's3_output_prefix': 's3://{bucket}/{prefix}',
            },
            'redshift': {
                'user': 'secret',
                'password': 'secret',
                'url': '{url}:{port}/{schema}',
            },
            'oracle': {
                'user': 'secret',
                'password': 'secret',
                'name': 'secret',
            },
            'mysql': {
                'user': 'secret',
                'password': 'secret',
                'name': 'secret',
                'url': '{url}:{port}',
            }
        }

    def _get_log(self):
        return {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {
                    'class': 'logging.Formatter',
                    'format': '%(asctime)s %(levelname)-8s %(module)-15s %(funcName)-20s %(message)s',
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'standard',
                },
                'file': {
                    'class': 'logging.FileHandler',
                    'formatter': 'standard',
                    'filename': None,
                    'mode': 'w',
                },
            },
            'loggers': {
                'glue_athena_utils': {
                    'level': 'DEBUG',
                    'handlers': ['console', 'file'],
                },
            },
        }



class ConfigPaths:

    def __init__(self):
        self.repo = Path(__file__).absolute().parents[1]
        self.package = self.repo / 'glue_athena_utils'
        self.migrations = self.repo / 'tests' / 'data' / 'resources' / 'migrations'
        self.sql = self.repo / 'tests' / 'data' / 'resources' / 'sql'
        self.logs = self.repo / 'tests' / 'data' / 'temp' / 'logs'
        self.mock_databases = self.repo / 'tests' / 'data' / 'temp' / 'mock_databases'
        self.mock_s3 = self.repo / 'tests' / 'data' / 'temp' / 'mock_s3'
        self.tests = self.repo / 'tests' / 'data' / 'temp' / 'tests'



config = Config()
