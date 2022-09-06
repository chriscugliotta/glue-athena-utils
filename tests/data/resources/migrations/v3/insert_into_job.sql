insert into test_job
select
    job_start_time,
    job_end_time,
    job_elapsed_time,
    job_status,
    case when job_status = 'complete' then 'A' else 'B' end as tenant_id,
    job_index,
    job_id
from test_job__backup
where {{chunk}}
