import pandas
import pytest
from datetime import datetime
from tests.config import config



@pytest.mark.parametrize('db_id', ['sqlite', 'glue', 'redshift', 'oracle'])
@pytest.mark.parametrize('empty', [False, True])
def test_select(databases, db_id, empty):
    """Tests a SQL select statement."""
    sql, expected_shape, expected_types = _get_sql(db_id, empty)
    db = databases[db_id]
    df = db.select(sql=sql, data_types=expected_types)
    assert df.shape == expected_shape
    assert all([df[column].dtype == data_type for column, data_type in expected_types.items()])
    assert empty or df['column2'][0] == 3.142


@pytest.mark.parametrize('db_id', ['sqlite', 'glue', 'oracle'])
def test_execute(databases, db_id):
    """Tests a sequence of SQL/DDL statements."""
    sql = 'create table test_execute as ' + _get_sql(db_id)[0] + ' -- !break\n'
    sql += 'drop table test_execute'
    db = databases[db_id]
    db.execute(sql=sql)


@pytest.mark.parametrize('db_id', ['sqlite', 'glue', 'oracle'])
@pytest.mark.parametrize('empty', [False, True])
def test_cache(databases, db_id, empty):
    sql, expected_shape, expected_types = _get_sql(db_id, empty)
    cache_path = config.paths.tests / 'test_database' / f'test_cache_{db_id}_{empty}.parquet'
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.is_file():
        cache_path.unlink()
    db = databases[db_id]
    df = db.cache(sql=sql, cache_path=cache_path, data_types=expected_types)
    assert df.shape == expected_shape
    assert all([df[column].dtype == data_type for column, data_type in expected_types.items()])
    assert empty or df['column2'][0] == 3.142
    cache_path.unlink()


@pytest.mark.parametrize('db_id', ['sqlite', 'glue'])
def test_insert_delete(databases, db_id):

    # Initialize.
    db = databases[db_id]
    df = _get_data()
    jinja_context = {'s3_database_prefix': db.s3_database_prefix}

    # Insert partitions 1 and 2.
    db.execute(sql=config.paths.sql / f'table_{db_id}.sql', jinja_context=jinja_context)
    db.insert(df=df.copy(), table_name='test_table', mode='overwrite_partitions', partition_columns=['column5', 'column6'])

    # Verify data.
    x = db.select(sql='select * from test_table order by column1')
    assert x.shape == (3, 6)
    assert x['column2'].tolist() == [3.142, 2.718, -0.12]

    # Overwrite partition 1.
    df = df[df['column6'] == 1]
    df['column2'] = 0
    db.insert(df=df.copy(), table_name='test_table', mode='overwrite_partitions', partition_columns=['column5', 'column6'])

    # Verify data.
    x = db.select(sql='select * from test_table order by column1')
    assert x.shape == (3, 6)
    assert x['column2'].tolist() == [0, 0, -0.12]

    # Delete all partitions.
    db.delete_partitions(table_name='test_table', partition_values=[('column5', 't')], safe_word='chris/')

    # Verify data.
    x = db.select(sql='select * from test_table order by column1')
    assert x.shape[0] == 0

    # Clean up.
    db.execute(sql='drop table test_table')


def test_render(databases):
    """Tests 'jinja' SQL renderer."""
    sql = '''
    select '{{value1}}' as column1
    {% if value2 %}
    union all select '{{value2}}' as column1
    {% endif %}
    '''
    jinja_context = {
        'value1': 'Hello world',
        'value2': False
    }
    db = databases['sqlite']
    df = db.select(sql=sql, jinja_context=jinja_context)
    assert df.shape == (1, 1)
    assert df['column1'][0] == 'Hello world'


def _get_sql(db_id, empty=False):

    sql = '\n'.join([
        "select",
        "    1 as column1,",
        "    3.142 as column2,",
        "    'A' as column3,",
        "    current_timestamp as column4",
    ])
    if db_id in ('oracle', 'mysql'):
        sql += '\nfrom dual'
    if empty:
        sql += '\nwhere 1 = 0'
    if db_id == 'glue':
        sql = sql.replace('current_timestamp', 'cast(current_timestamp as timestamp)')

    expected_shape = (0 if empty else 1, 4)

    expected_types = {
        'column1': int,
        'column2': float,
        'column3': object,
        'column4': 'datetime64[ns]',
    }

    return sql, expected_shape, expected_types


def _get_data():
    rows = [
        (1, 3.142, 'A', datetime(1985, 11, 27, 0, 0, 1), 't', 1),
        (2, 2.718, 'B', datetime(1985, 11, 27, 0, 1, 0), 't', 1),
        (3, -0.12, 'C', datetime(1985, 11, 27, 1, 0, 0), 't', 2),
    ]
    columns = [
        'column1',
        'column2',
        'column3',
        'column4',
        'column5',
        'column6',
    ]
    return pandas.DataFrame(rows, columns=columns)
