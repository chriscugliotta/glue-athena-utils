import awswrangler as wr
import pandas
from datetime import datetime, timedelta
from random import randint



def upgrade(db, dir, env):

    # Create `JOB` table.
    db.execute(
        sql=dir / 'create_table_job.sql',
        jinja_context={'db': db}
    )

    # Insert records into `JOB` table.
    db.insert(
        df=_get_data(),
        table_name='test_job',
        partition_columns=['job_index', 'job_id'],
        mode='append',
    )


def downgrade(db, dir, env):

    # Delete records from `JOB` table.
    if db.type == 'glue':
        wr.s3.delete_objects(
            path=f'{db.s3_database_prefix}/test_job/',
            boto3_session=db.boto3_session,
        )

    # Drop `JOB` table.
    db.execute(sql='drop table test_job')


def _get_data():
    start_times = [
        datetime(2020, 1, 1, 0, 0, 0) + timedelta(days=i, seconds=randint(0, 60*60*24 - 1))
        for i in range(10)
    ]
    rows = [
        {
            'job_start_time': start_time,
            'job_end_time': start_time + timedelta(seconds=randint(0, 60*60 - 1)),
            'job_status': 'complete' if randint(0, 1) else 'error',
            'job_index': start_time.strftime('%Y%m'),
            'job_id': start_time.strftime('%Y-%m-%d-%H-%M-%S') + str(randint(0, 999999)).zfill(6),
        }
        for start_time in start_times
    ]
    return pandas.DataFrame(rows)
