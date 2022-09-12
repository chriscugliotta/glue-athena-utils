from glue_athena_utils.database.backup_drop_rebuild import backup_drop_rebuild



def upgrade(db, dir, env):

    # Alter `JOB` table.  Add `JOB_ELAPSED_TIME` column.
    backup_drop_rebuild(
        db=db,
        table_name='test_job',
        partition_columns=['job_index', 'job_id'],
        create_sql=dir / 'create_table_job.sql',
        insert_sql=dir / 'insert_into_job.sql',
        jinja_context={'db': db},
        chunk_size=7,
    )


def downgrade(db, dir, env):

    # Alter `JOB` table.  Remove `JOB_ELAPSED_TIME` column.
    backup_drop_rebuild(
        db=db,
        table_name='test_job',
        partition_columns=['job_index', 'job_id'],
        create_sql=dir.parent / 'v1' / 'create_table_job.sql',
        insert_sql=dir / 'insert_into_job_downgrade.sql',
        jinja_context={'db': db},
        chunk_size=7,
    )
