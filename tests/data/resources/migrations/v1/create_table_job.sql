{% if db.type == 'glue' %}
CREATE EXTERNAL TABLE test_job (
    job_start_time timestamp,
    job_end_time timestamp,
    job_status string)
PARTITIONED BY (
    job_index string,
    job_id string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    '{{db.s3_database_prefix}}/test_job/'
TBLPROPERTIES (
    'classification'='parquet',
    'compressionType'='snappy')

{% else %}
CREATE TABLE test_job (
    job_start_time timestamp,
    job_end_time timestamp,
    job_status string,
    job_index string,
    job_id string)

{% endif %}
