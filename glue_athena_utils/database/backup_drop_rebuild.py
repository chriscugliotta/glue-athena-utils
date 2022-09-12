import awswrangler as wr
import logging
from pathlib import Path
from typing import Any, Dict, List
from glue_athena_utils.database.connection import DatabaseConnection
log = logging.getLogger(__name__)





class Table:

    def __init__(self, name: str, **args):
        self.name: str = name
        self.partition_columns: List[str] = args.get('partition_columns', [])
        self.partition_columns_quoted: str = ', '.join(["'" + x + "'" for x in self.partition_columns])
        self.partition_columns_nonquoted: str = ', '.join(self.partition_columns)
        self.partition_values: List[dict] = None
        self.s3_path: str = args.get('s3_path')
        self.backup_name: str = self.name + '__backup'
        self.backup_s3_path: str = args.get('backup_s3_path')
        self.jinja_context: Dict[str, str] = args.get('jinja_context', {})





def backup_drop_rebuild(db: DatabaseConnection, table_name: str, create_sql: Path, insert_sql: Path, chunk_size: int = 100, **args):
    """
    Modifies a Glue table, and _also_ modifies the underlying data files on S3.

    This function aims to address two major limitations of Amazon Athena:

        1.  The absence of SQL `update` command.
        2.  Athena's 100-partition limit for CTAS and `insert` commands.

    Use case:  Suppose we have a gigantic archive table with >100 partitions.  Additionally, suppose
    business requirements have changed unexpectedly, such that we need to modify the structure of
    our table.  In a traditional RDBMS, we might use SQL `alter` and/or `update` statements to
    modify the table.  Alternatively, we might create a new, modified (and empty) table, and then
    insert (and appropriately transform) the old records into it.  Unfortunately, neither approach
    is possible in Athena, due to the limitations listed above.  Thus, this function aims to
    streamline a workaround.

    The algorithm can be summarized as follows:

        1.  Create backup table.
        2.  Drop old table (and S3 data).
        3.  Create new, modified, empty table.
        4.  Insert (and transform) backed-up records into new table.
        5.  Drop backup table (and S3 data).

    Note that, for partitioned tables, steps 1 and 4 are processed in chunks, i.e. we loop and
    insert 100 partitions at a time.

    Args:
        db (DatabaseConnection):
            Database interface.

        table_name (str):
            Name of Glue table to modify.

        create_sql (Path or str):
            Path of SQL script.  Used in step 3 above.  It should contain the new, modified table's
            `create table` DDL.

        insert_sql (Path or str):
            Path of SQL script.  Used in step 4 above.  It should implement the SQL statement that
            inserts (and possibly transforms) records from the backup table into the new table.
            Note that, for partitioned tables, this insert statement will be looped and executed
            multiple times, i.e. once per chunk of 100 partitions.  Thus, the SQL `where` clause
            must contain the keyword `{{chunk}}`.  For example:

            ```sql
            insert into table
            select *
            from table__backup
            where {{chunk}}
            ```

            At runtime, this keyword will be replaced with a dynamically-generated `where` clause
            that targets at most 100 partitions, thereby avoiding Athena's limitation.

        chunk_size (int):
            Maximum number of partitions inserted per iteration.

    Note:
        Currently, this code has _limited_ support for partition structure changes.  Such an
        operation is possible, but it may violate the 100-partition limit.  As a known limitation,
        this code will always define chunks in terms of the _old_ partition structure.  For example,
        suppose we want to change our partition structure from `YYYY` to `YYYYMM`.  First, this code
        will define chunks, such that each chunk contains (at most) 100 distinct `YYYY` values.
        Then, when it tries inserting 100 `YYYY` values into the new table, this ends up producing
        1200 `YYYYMM` partitions, which violates the limit.  To avoid this, we can manually override
        the `chunk_size` argument to 8.  This is the best I could do for now...

    References:
        Athena's 100-partition limit:
        https://docs.aws.amazon.com/athena/latest/ug/ctas-insert-into.html
    """
    table = _describe_table(db, table_name, **args)
    _backup_table(db, table, chunk_size)
    _drop_table(db, table, backup=False)
    _rebuild_table(db, table, create_sql, insert_sql, chunk_size)
    _drop_table(db, table, backup=True)


def _describe_table(db: DatabaseConnection, table_name: str, **args) -> Table:

    # Use boto3 to look up the Glue table's S3 path.
    if db.type == 'glue':
        glue = db.boto3_session.client('glue')
        response = glue.get_table(DatabaseName=db.name, Name=table_name)
        args['partition_columns'] = [x['Name'] for x in response['Table'].get('PartitionKeys', [])]
        args['s3_path'] = response['Table']['StorageDescriptor']['Location']
        args['backup_s3_path'] = args['s3_path'].rsplit('/', 1)[0] + f'/{table_name}__backup'

    # Instantiate and return Table object.
    table = Table(table_name, **args)
    table.partition_values = _get_partition_values(db, table)
    return table


def _get_partition_values(db: DatabaseConnection, table: Table) -> List[dict]:

    if len(table.partition_columns) == 0:
        return None

    # Get distinct partition values.
    sql = [
        f"select distinct {table.partition_columns_nonquoted}",
        f"from {table.name}",
        f"order by {table.partition_columns_nonquoted}"
    ]
    sql = '\n'.join(sql)
    df = db.select(sql=sql)

    # Convert dataframe to list of dicts.  Each dict represents a single partition.
    return [
        {
            column: row[i+1]
            for i, column in enumerate(table.partition_columns)
        }
        for row in df.itertuples()
    ]


def _backup_table(db: DatabaseConnection, table: Table, chunk_size: int):

    # Create empty backup table.
    sql = [
        f"create table {table.backup_name}",
        f"with (",
        f"    external_location = '{table.backup_s3_path}',",
        f"    partitioned_by = array[{table.partition_columns_quoted}]",
        f")",
        f"as select *",
        f"from {table.name}",
        f"where 1 = 0"
    ]
    if db.type == 'sqlite': del sql[1:5]
    sql = '\n'.join(sql)
    db.execute(sql=sql)

    # Get insert SQL.
    sql = [
        f"insert into {table.backup_name}",
        f"select *",
        f"from {table.name}",
        f"where " + "{{chunk}}",
    ]
    sql = '\n'.join(sql)

    # Populate backup table.
    _insert_chunks(db, table, sql, chunk_size, backup=True)


def _insert_chunks(db: DatabaseConnection, table: Table, insert_sql: str, chunk_size: str, backup: bool):

    # Divide partitions into chunks.  (Each chunk will contain <=100 partitions.)
    if table.partition_values is not None:
        chunks = _get_chunks(table.partition_values, chunk_size)
    else:
        chunks = [None]

    # Loop and insert.
    for i, chunk in enumerate(chunks):

        # Get SQL `where` clause that targets given chunk.
        if chunk is not None:
            chunk_sql = _get_chunk_sql(chunk)
        else:
            chunk_sql = '1 = 1'

        # Insert given chunk.
        db.execute(
            sql=insert_sql,
            jinja_context={**table.jinja_context, **{'chunk': chunk_sql}},
            quiet=(i > 0),
        )
        log.debug(f'Inserted chunk {i+1} of {len(chunks)} into table:  {table.backup_name if backup else table.name}.')


def _get_chunks(l: List[Any], n: int) -> List[List[Any]]:
    """
    Divides a list into chunks of size `n`.

    References:
        Code taken from:
        https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
    """
    return [l[i:i+n] for i in range(0, len(l), n)]


def _get_chunk_sql(partition_values: List[dict]) -> str:
    """Dynamically generates a SQL `where` clause that targets the given partition list."""
    s = '1 = 0'
    for partition_value in partition_values:
        s += '\n    or ('
        for j, (column, value) in enumerate(partition_value.items()):
            s += '' if j == 0 else ' and '
            s += f"{column} = '{value}'"
        s += ')'
    return s


def _drop_table(db: DatabaseConnection, table: Table, backup: bool):

    table_name = table.backup_name if backup else table.name
    s3_path = table.backup_s3_path if backup else table.s3_path

    # Important!  Make for-damn-sure there is a trailing slash.
    # Otherwise, both original _and_ backup will get deleted!  (Data is gone forever.)
    if s3_path is not None and s3_path[-1] != '/':
        s3_path += '/'

    # Delete S3 data.
    if db.type == 'glue':
        log.debug(f'Deleting S3 data at:  {s3_path}.')
        wr.s3.delete_objects(path=s3_path, boto3_session=db.boto3_session)

    # Drop table.
    log.debug(f'Dropping table:  {table_name}.')
    db.execute(sql=f'drop table {table_name}', quiet=True)


def _rebuild_table(db: DatabaseConnection, table: Table, create_sql: str, insert_sql: str, chunk_size: int):

    # Read `create_sql` script.
    if isinstance(create_sql, Path):
        with open(create_sql, 'r') as file:
            create_sql = file.read()

    # Read `insert_sql` script.
    if isinstance(insert_sql, Path):
        with open(insert_sql, 'r') as file:
            insert_sql = file.read()

    # Create empty new table.
    db.execute(sql=create_sql, jinja_context=table.jinja_context)

    # Populate new table.
    _insert_chunks(db, table, insert_sql, chunk_size, backup=False)
