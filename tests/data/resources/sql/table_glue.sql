CREATE EXTERNAL TABLE test_table (
    column1 int,
    column2 double,
    column3 string,
    column4 timestamp)
PARTITIONED BY (
    column5 string,
    column6 string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    '{{s3_database_prefix}}/test_table'
TBLPROPERTIES (
    'classification'='parquet',
    'compressionType'='snappy')
