import awswrangler as wr
import boto3
import jinja2
import logging
import pandas
import random
import sqlalchemy
import time
from awswrangler.exceptions import QueryFailed
from boto3.session import Session
from botocore.exceptions import ClientError
from pandas import DataFrame
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from glue_athena_utils.s3.connection import S3Connection
log = logging.getLogger(__name__)



class DatabaseConnection:
    """
    An abstract database interface.

    Attributes:
        type (str):
            Database type, e.g. 'glue' or `sqlite'.

        user (str):
            Database user or schema name.

        password (str):
            Database password.

        name (str):
            Database instance name.

        url (str):
            Database URL.

        jinja_environment (jinja2.Environment):
            Jinja configuration object.  Can optionally be overridden.

        sqlalchemy_engine (sqlalchemy.engine.Engine):
            SQLAlchemy engine.

        boto3_session (boto3.session.Session):
            Athena queries will use this boto3 session object.

        s3_database_prefix (str):
            Athena queries will target Glue tables at this S3 prefix.

        s3_output_prefix (str):
            Athena query results will be placed at this S3 prefix.

        athena_workgroup (str):
            Athena queries will be executed in this workgroup.

        max_attempts (int):
            The maximum number of times each SQL statement is attempted.

        delay (int):
            The amount of time in seconds to wait between attempts.
            This wait time will double after each failed attempt.

        attach (List[dict]):
            List of SQLite databases that will be attached prior to every `select` and `execute`
            statement.

    Note:
        Under the hood, this class is powered by SQLAlchemy.  SQLAlchemy lets us interact with a
        wide variety of databases.  Also, Pandas DataFrames can be instantiated directly from a
        SQLAlchemy query result.

    Note:
        At time of writing, SQLAlchemy does not support Athena.  Thus, when interacting with Athena,
        this class uses AWS Data Wrangler library instead of SQLAlchemy.

    References:
        SQLAlchemy engines and connections:
        http://docs.sqlalchemy.org/en/rel_1_1/core/engines.html
        http://docs.sqlalchemy.org/en/rel_1_1/core/connections.html

        Jinja API and template syntax:
        http://jinja.pocoo.org/docs/2.10/api/
        http://jinja.pocoo.org/docs/2.10/templates/
    """

    def __init__(self, **args):
        self.type: str = args.get('type')
        self.user: str = args.get('user')
        self.password: str = args.get('password')
        self.name: str = args.get('name')
        self.url: str = args.get('url')
        self.jinja_environment: jinja2.Environment = args.get('jinja_environment', self._get_default_jinja_environment())
        self.sqlalchemy_engine: sqlalchemy.engine.Engine = self._get_sqlalchemy_engine()
        self.boto3_session: Session = args.get('boto3_session')
        self.s3_database_prefix: str = args.get('s3_database_prefix')
        self.s3_output_prefix: str = args.get('s3_output_prefix')
        self.s3_connection: S3Connection = self._get_s3_connection()
        self.athena_workgroup: str = args.get('athena_workgroup')
        self.max_attempts: int = args.get('max_attempts', 1)
        self.delay: int = args.get('delay', 1)
        self.attach: List[Dict[str, str]] = args.get('attach', [])
        log.info(f'Constructed new DatabaseConnection!  user = {self.user}, name = {self.name}')

    def __str__(self):
        return 'f{self.user}@{self.name}'

    def _get_default_jinja_environment(self) -> jinja2.Environment:
        return jinja2.Environment(trim_blocks=True, lstrip_blocks=True)

    def _get_sqlalchemy_engine(self) -> sqlalchemy.engine.Engine:
        if self.type != 'glue':
            return sqlalchemy.create_engine(self._get_sqlalchemy_connection_string())

    def _get_sqlalchemy_connection_string(self) -> str:
        """Builds a SQLAlchemy-compatible database connection string."""
        if self.type == 'oracle':
            return 'oracle+cx_oracle://{0}:{1}@{2}'.format(self.user, self.password, self.name)
        elif self.type == 'mysql':
            return 'mysql+pymysql://{0}:{1}@{2}/{3}'.format(self.user, self.password, self.url, self.name)
        elif self.type == 'redshift':
            return 'postgresql://{0}:{1}@{2}'.format(self.user, self.password, self.url)
        elif self.type == 'sqlite':
            return 'sqlite://{0}'.format('' if self.url is None else '/' + str(self.url))
        else:
            raise NotImplementedError

    def _get_s3_connection(self) -> S3Connection:
        if self.type == 'glue':
            return S3Connection(
                bucket_name=self.s3_database_prefix.split('/')[2],
                boto3_session=self.boto3_session,
            )

    def select(
        self,
        sql: str,
        cache_path: Path = None,
        jinja_context: Dict = {},
        data_types: Dict[str, str] = None,
        quiet: bool = False,
    ) -> DataFrame:
        """
        Executes a SQL select statement.

        Args:
            sql (str or Path):
                A SQL statement (or file) to execute.

            cache_path (Path):
                If provided, the query result will be cached as this parquet file.

            jinja_context (Dict):
                A dictionary containing all variable names (and values) passed into Jinja template.

            data_types (Dict[str, str]):
                Mapping between column names and data types.
                Applied to dataframe after reading SQL query.

        Returns:
            DataFrame:  Contains query result.
        """

        # If SQL path was given, get file content.
        if isinstance(sql, Path):
            with open(sql, 'r') as file:
                sql = file.read()

        # Render Jinja template into pure SQL.
        sql = self._render(sql, jinja_context)

        # Log.
        if not quiet:
            log.debug(f'Executing SQL...\n{sql}')

        # Get query result as dataframe.
        if self.type != 'glue':
            df = self._retry(self.max_attempts, self.delay, self._select_alchemy, sql=sql)
        else:
            df = self._retry(self.max_attempts, self.delay, self._select_wrangler, sql=sql, data_types=data_types)

        # Convert data types.
        if data_types is not None:
            df = df.astype({k: v for k, v in data_types.items() if k in df})

        # Cache.
        if cache_path is not None:
            df.to_parquet(cache_path, index=False)

        # Log, return.
        if not quiet:
            log.debug(f'Done with row count = {len(df):,}.')
        return df

    def _select_alchemy(self, sql: str) -> DataFrame:
        """Executes a SQL select statement via SQLAlchemy."""
        with self.sqlalchemy_engine.begin() as connection:
            if self.type == 'sqlite': self._attach_sqlite_databases(connection)
            return pandas.read_sql(sql, connection)

    def _select_wrangler(self, sql: str, data_types: Dict[str, str]) -> DataFrame:
        """Executes a SQL select statement via Wrangler."""
        df = wr.athena.read_sql_query(
            sql=sql,
            boto3_session=self.boto3_session,
            database=self.name,
            s3_output=self.s3_output_prefix,
            workgroup=self.athena_workgroup,
            keep_files=False,
        )
        if df.shape == (0, 0):
            df = DataFrame(columns=data_types).astype(data_types)
        return df

    def _render(self, sql: str, jinja_context: Dict) -> str:
        """Uses Jinja to render the SQL template into pure SQL."""
        return self.jinja_environment.from_string(sql).render(**jinja_context)

    def _retry(self, max_attempts: int, delay: int, func: Callable, **args) -> Any:
        """
        Repeatedly attempts to execute a SQL statement.  Each retry is delayed.

        Note:
            The primary purpose of this function is to overcome 'transient' errors, such as
            Athena concurrent query limit violations, or vague internal AWS errors.  For example,
            we will not retry queries that fail with a SQL syntax error.

        References:
            Exponential backoff and jitter:
            https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        """

        def is_transient(e):
            if isinstance(e, ClientError) and e.response['Error']['Code'] == 'TooManyRequestsException':
                return True
            if isinstance(e, QueryFailed) and 'INTERNAL_ERROR_QUERY_ENGINE' in e.args[0]:
                return True
            if isinstance(e, QueryFailed) and 'Query exhausted resources at this scale factor' in e.args[0]:
                return True
            else:
                return False

        for i in range(max_attempts):
            try:
                return func(**args)
            except Exception as e:
                if is_transient(e) and i < max_attempts - 1:
                    sleep_time = min(15 * 60, delay * (2 ** i))
                    sleep_time *= random.uniform(0.5, 1)
                    log.exception('Will try again in {} seconds...'.format(sleep_time))
                    time.sleep(sleep_time)
                else:
                    raise e

    def execute(self, sql: str, jinja_context: Dict = {}, quiet: bool = False):
        """
        Executes an arbitrary SQL statement.

        Note:
            Optionally, multiple statements can be batched together.  The statements should be
            separated with '-- !break' delimiter.

        Args:
            sql (str):
                A SQL statement (or file) to execute.

            jinja_context (Dict):
                A dictionary containing all variable names (and values) passed into Jinja template.
        """

        # If SQL path was given, get file content.
        if isinstance(sql, Path):
            with open(sql, 'r') as file:
                sql = file.read()

        # Render Jinja template into pure SQL.
        sql = self._render(sql, jinja_context)

        # Split.
        for statement in sql.split('-- !break'):

            # Strip leading and trailing whitespace.
            statement = statement.strip()

            # Log.
            if not quiet:
                log.debug(f'Executing SQL...\n{statement}')

            # Execute.
            if self.type != 'glue':
                self._retry(self.max_attempts, self.delay, self._execute_alchemy, sql=statement)
            else:
                self._retry(self.max_attempts, self.delay, self._execute_wrangler, sql=statement)

        # Log.
        if not quiet:
            log.debug('Done.')

    def _execute_alchemy(self, sql):
        """Executes an arbitrary SQL statement via SQLAlchemy."""
        with self.sqlalchemy_engine.begin() as connection:
            if self.type == 'sqlite': self._attach_sqlite_databases(connection)
            connection.execute(sql)

    def _execute_wrangler(self, sql):
        """Executes an arbitrary SQL statement via Wrangler."""
        query_id = wr.athena.start_query_execution(
            sql=sql,
            boto3_session=self.boto3_session,
            database=self.name,
            s3_output=self.s3_output_prefix,
            workgroup=self.athena_workgroup,
        )
        query_status = wr.athena.wait_query(
            query_execution_id=query_id,
            boto3_session=self.boto3_session
        )

    def insert(self, **args):
        """Inserts a dataframe into a database table."""
        if self.type == 'glue':
            self._insert_glue(**args)
        elif self.type == 'sqlite':
            self._insert_sqlite(**args)
        else:
            raise NotImplementedError

    def _insert_glue(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = 'overwrite_partitions',
        partition_columns: List[str] = None,
        s3_table_prefix: str = None,
    ):
        """
        Inserts a dataframe into a Glue table via Wrangler.

        Args:
            df (DataFrame):
                Dataframe to insert.

            table_name (str):
                Glue table name.

            mode (str):
                Either `append`, `overwrite`, or `overwrite_partitions` (default).
                See Wrangler documentation for details.

            partition_columns (List[str]):
                Glue table partition columns.

            s3_table_prefix (str):
                Glue table S3 prefix.
        """

        if s3_table_prefix is None:
            s3_table_prefix = f'{self.s3_database_prefix}/{table_name}'

        log.debug(f'Inserting {len(df):,} record(s) into table:  {table_name}.')
        x = 1
        return wr.s3.to_parquet(
            df=df,
            path=s3_table_prefix,
            dataset=True,
            boto3_session=self.boto3_session,
            database=self.name,
            table=table_name,
            partition_cols=partition_columns,
            schema_evolution=False,
            mode=mode,
        )

    def _insert_sqlite(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = 'overwrite_partitions',
        partition_columns: List[str] = None,
        **args
    ):
        """
        Inserts a dataframe into a SQLite table via an interface that emulates Wrangler.

        Note:
            SQLite doesn't actually support partitioning.  However, in some applications, we speed
            up our unit tests by mocking out S3/Glue/Athena database with a local SQLite instance.
            The purpose of this function is to give our Athena and SQLite connections a common
            interface, so we can use them interchangeably.
        """

        # Emulate Wrangler `mode='overwrite'` behavior in SQLite.
        if mode == 'overwrite':
            sql = 'truncate table {}'.format(table_name)
            self.execute(sql=sql)

        # Emulate Wrangler `mode='overwrite_partitions'` behavior in SQLite.
        if mode == 'overwrite_partitions':
            sql = 'delete from {} where 1 = 1'.format(table_name)
            for column in partition_columns:
                values = ','.join(["'" + str(x).replace("'", "''") + "'" for x in df[column].drop_duplicates().sort_values()])
                sql += ' and {} in ({})'.format(column, values)
            self.execute(sql=sql)

        # Append dataframe into SQLite table.
        log.debug(f'Inserting {len(df):,} record(s) into table:  {table_name}.')
        with self.sqlalchemy_engine.begin() as con:
            df.to_sql(table_name, con=con, index=False, if_exists='append')

    def delete_partitions(self, **args):
        """Deletes partitions from a database table."""
        if self.type == 'glue':
            return self._delete_partitions_glue(**args)
        elif self.type == 'sqlite':
            return self._delete_partitions_sqlite(**args)
        else:
            raise NotImplementedError

    def _delete_partitions_glue(self, table_name: str, partition_values: List[Tuple[str, str]], safe_word: str = None, s3_table_prefix: str = None) -> Dict:
        """
        Deletes all Glue partitions and corresponding S3 data for a given table and partition path.

        Args:
            table_name (str):
                Glue table name.

            partition_values (List[Tuple[str, str]]):
                List of partition column names and values, e.g. `[('job_index', 't'), ('job_id', 'test')]`.

            s3_table_prefix (str):
                Glue table S3 prefix.

            safe_word (str, optional):
                A safety feature intended to prevent catastrophic, accidental deletion of data.
                If any given key does NOT contain this substring, an exception is raised.

        Returns:
            Dict:  Contains deleted Glue partitions and S3 objects.
        """

        if s3_table_prefix is None:
            s3_table_prefix = '{0}/{1}'.format(self.s3_database_prefix, table_name)

        # We will delete all files at `s3_delete_prefix`.
        s3_partition_suffix = '/'.join([f'{name}={value}' for name, value in partition_values])
        s3_delete_prefix =  f'{s3_table_prefix}/{s3_partition_suffix}/'
        s3_delete_prefix = s3_delete_prefix.replace(f's3://{self.s3_connection.bucket_name}/', '')

        # If no partitions are specified, everything is deleted.
        if partition_values == []:
            s3_delete_prefix = s3_delete_prefix[:-1]

        # Get in-scope S3 objects.
        s3_objects = self.s3_connection.list(prefix=s3_delete_prefix)

        # Get in-scope Glue partitions.
        glue_partitions = wr.catalog.get_parquet_partitions(
            table=table_name,
            database=self.name,
            expression=' and '.join([f"{name}='{value}'" for name, value in partition_values]),
            boto3_session=self.boto3_session,
        )

        # Log.
        log.debug('Deleting {0:,} Glue partition(s), {1:,} S3 object(s), {2:0.2f} MiB at:  {3}.'.format(
            len(glue_partitions),
            len(s3_objects),
            sum(x['Size'] for x in s3_objects) / 1024 / 1024,
            s3_delete_prefix,
        ))

        # Delete in-scope S3 objects.
        self.s3_connection.delete(keys=[x['Key'] for x in s3_objects], safe_word=safe_word, quiet=True)

        # Delete in-scope Glue partitions.
        wr.catalog.delete_partitions(
            database=self.name,
            table=table_name,
            partitions_values=list(glue_partitions.values()),
            boto3_session=self.boto3_session,
        )

        # Return deleted object metadata.
        return {
            'glue_partitions': glue_partitions,
            's3_objects': s3_objects,
        }

    def _delete_partitions_sqlite(self, table_name: str, partition_values: List[Tuple[str, str]], **args):
        """
        Deletes all partitions from a SQLite table via an interface that emulates Wrangler.

        Note:
            SQLite doesn't actually support partitioning.  However, in some applications, we speed
            up our unit tests by mocking out S3/Glue/Athena database with a local SQLite instance.
            The purpose of this function is to give our Athena and SQLite connections a common
            interface, so we can use them interchangeably.
        """
        sql = 'delete from {} where 1 = 1'.format(table_name)
        for name, value in partition_values:
            sql += " and {} = '{}'".format(name, str(value).replace("'", "''"))
        self.execute(sql=sql)

    def cache(
        self,
        sql: str,
        cache_path: Path,
        cache_refresh: str = 'if_needed',
        jinja_context: Dict = {},
        data_types: Dict[str, str] = None,
    ) -> DataFrame:
        """
        Many times, loading from a remote database (e.g. Oracle) is extremely slow.  For instance,
        if a query returns millions of rows, it can easily take an hour or more to download the
        whole thing.  In a typical ETL app, the first step is to launch a bunch of queries.  This is
        a huge bottleneck, and makes it difficult to test later stages of the code.  Thus, as a
        performance optimization, this function implements query caching.  Each query passes through
        this function, which has the following steps:

            1.  Render the SQL query via Jinja.

            2.  Execute our SQL 'select' statement.

            3.  Cache the result locally (at `cache_path`).

            4.  Read the cached result into a dataframe.

        Only step #2 is slow. Everything else is fast.

        Now, suppose we want to isolate and test a later stage of the program (i.e. unit testing,
        tinkering with a new feature, etc.).  In this case, we don't want to wait for step #2 each
        time.  Instead, if our cache files already exist, we can `refresh_cache=never`, and then
        steps 1-3 will be skipped! Step 4 will still run smoothly, and the rest of the program
        won't know the difference. This greatly improves the speed of testing.

        Args:
            Same arguments as `select` method, but also:

            cache_refresh (str):
                Possible values:  always, never, if_needed.
                If `always`, SQL query is always executed (cache file is created or overwritten).
                If `never`, SQL query is never executed (cached result is returned instead).
                If 'if_needed', SQL query is skipped whenever the result is already cached locally.

        Returns:
            DataFrame:  Contains query result.
        """

        # If refresh_cache parameter is enabled, do steps 1-3.
        if (
            (cache_refresh == 'always') or
            (cache_refresh == 'if_needed' and not cache_path.is_file())
        ):
            df = self.select(sql, cache_path, jinja_context, data_types)
            del df

        # Do step 4.
        return pandas.read_parquet(cache_path)

    def _attach_sqlite_databases(self, connection: object):
        """Attaches SQLite databases to open connection."""
        for x in self.attach:
            connection.execute(f"attach database '{x['path']}' as {x['name']}")
