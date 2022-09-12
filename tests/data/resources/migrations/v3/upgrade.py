from glue_athena_utils.database.backup_drop_rebuild import backup_drop_rebuild



def upgrade(db, dir, env):

    # Alter `JOB` table.  Add `tenant_id` to partition columns.
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

    # Alter `JOB` table.  Remove `tenant_id` from partition columns.
    backup_drop_rebuild(
        db=db,
        table_name='test_job',
        partition_columns=['tenant_id', 'job_index', 'job_id'],
        create_sql=dir.parent / 'v2' / 'create_table_job.sql',
        insert_sql=dir / 'insert_into_job_downgrade.sql',
        jinja_context={'db': db},
        chunk_size=7,
    )
