insert into test_job
select
    job_start_time,
    job_end_time,
    {% if db.type == 'glue' %}
    cast(date_diff('second', job_start_time, job_end_time) as double) / 60 as job_elapsed_time,
    {% else %}
    (julianday(job_end_time) - julianday(job_start_time)) * 24 * 60 as job_elapsed_time,
    {% endif %}
    job_status,
    job_index,
    job_id
from test_job__backup
where {{chunk}}
