# aws-utils

This library aims to improve developer experience (DX) and productivity with AWS Glue and Athena.  It contains several utilities that help streamline a Python/SQL workflow, with a focus on fast automated testing, automated schema migrations, and overcoming AWS Athena's [update](https://stackoverflow.com/questions/71705848/aws-athena-update-table-rows-using-sql) and [100-partition](https://docs.aws.amazon.com/athena/latest/ug/ctas-insert-into.html) limitations.  **The target audience is people who prefer SQL over OOP** in the context of data processing.



# Contents

- [Getting Started](#getting-started)
- [Query Parameterization](#query-parameterization)
    - [Use Case](#use-case)
    - [Example](#example)
- [Query Testing](#query-testing)
    - [Use Case](#use-case-1)
    - [Example](#example-1)
- [Schema Migrations](#schema-migrations)
    - [Use Case](#todo)
    - [Example](#todo)
- [Overcoming Athena Limitations](#overcoming-athena-limitations)
    - [Use Case](#use-case-2)
    - [Example](#example-2)



## Getting Started

This library can be installed via pip:

```
pip install git+https://github.com/chriscugliotta/aws-utils.git
```

This library introduces a class named [`DatabaseConnection`](aws_utils/database/connection.py) which has two primary methods:

- [`select`](/aws_utils/database/connection.py#L136):  Executes a SQL statement and returns the query result as a Pandas dataframe.
- [`execute`](/aws_utils/database/connection.py#L256):  Executes arbitrary SQL on the database, e.g. inserts, DDL commands, etc.

Here is a syntax example:

```python
from aws_utils.database.connection import DatabaseConnection

db = DatabaseConnection(
    type='glue',
    name='my_db',
    athena_workgroup='my-workgroup',
)

df = db.select(
    sql='select * from my_table where job_id = {{job_id}}',
    jinja_context={'job_id': 100}
)
```



## Query Parameterization

#### Use Case

Suppose you maintain a standard SQL script, and suddenly need to accommodate a client customization, e.g. a loading filter that is only applicable to a single client.  In this case, consider using Jinja to conditionally inject a SQL `where` clause into your query.  This may seem ugly, but client customizations are always ugly, and this approach is a lot cleaner than forking your codebase for each client.

#### Example

The `DatabaseConnection` class can (optionally) use [Jinja2](https://jinja.palletsprojects.com/en/3.1.x/) to enable easy and expressive query parameterization.  Sometimes this approach is called "Jinja SQL."  Here is an example:

```python
from aws_utils.database.connection import DatabaseConnection

db = DatabaseConnection(
    type='glue',
    name='my_db',
    athena_workgroup='my-workgroup',
)

params = {
    'schema': 'my_schema',
    'job_ids': [100, 101, 102],
    'limit': None,
}

sql = '''
select *
from {{schema}}.my_table
where job_id in ({{ job_ids | join(',') }})
{% if limit is not none %}
limit {{limit}}
{% endif %}
'''

df = db.select(
    sql=sql,
    jinja_context=params
)
```

This will execute the following query:

```sql
select *
from my_schema.my_table
where job_id in (100,101,102)
```

Admittedly, the Jinja template syntax is pretty ugly.  However, the improved expressivity is sometimes worth the tradeoff.

> **NOTE:** You can use something like [J2Live](https://j2live.ttl255.com) to quickly validate the template syntax.



## Query Testing

#### Use Case

End-to-end testing is important for most ETL applications.  However, these tests are difficult to automate, because any realistic test will induce real strain on the database.  As a result, these tests are typically slow and potentially dangerous, i.e. capable of crashing a live preprod environment that other people depend on.

Typically, I try to design my test suite with two modes:  slow and fast.  In slow mode, the end-to-end tests are as realistic as possible.  In fast mode, various components are disabled or mocked out, which drastically improves performance at the expense of test coverage, e.g. 1 minute vs. 1 hour.  For example, I prefer fast tests for quick feedback (per git commit), but then use slow tests when merging new code into production.

Fast automated testing is very important for developer productivity.  If your tests take >1 hour, then you can easily waste an entire day debugging via trial-and-error, e.g. fix a single mistake, push to git, wait ~1 hour for CI/CD, repeat.  This is miserable.  **If you want to gets things done quickly, then your tests must also be fast!**

However, fast tests are difficult to achieve in the ETL context.  If our pipeline is bottlenecked by a sequence of slow, gargantuan SQL queries, how can the test be fast?  One option is to skip the queries entirely (and load mock results via local CSVs), but then you lose test coverage on your SQL queries.  This arguably defeats the purpose of testing when the majority of your codebase is SQL.

So, how do we get fast tests *and* good coverage?

In one project, I achieved this by creating a SQLite replication of the production database (which was AWS Glue).  This required some prep work, but the end result was worth it.  The SQLite database contained the same table schema as Glue, but with only a small subset of rows.  Then, all SQL queries were carefully written to be compatible with both Athena and SQLite (using Jinja to reconcile any syntax differences).  Fortunately, Athena and SQLite syntax are almost identical.

So, slow tests can target the (real) Glue database, whereas fast tests can be redirected to the SQLite database.  In my particular project, this reduced runtime by several orders of magnitude, without any reduction in code coverage.  It also drastically sped up day-to-day development, e.g. hitting F5 in an IDE became snappy without any network delay, which is a beautiful thing.  It also allowed us to work offline or during a VPN outage.  Overall, my team considered this a big success.

#### Example

```python
from aws_utils.database.connection import DatabaseConnection

def get_db(mode):
    if mode == 'fast':
        return DatabaseConnection(type='sqlite', url='path/to/my.db')
    else:
        return DatabaseConnection(type='glue', name='my_db', athena_workgroup='my-workgroup')

sql = '''
select *
from my_table
{% if mode == 'fast' %}
where sales_date = '2022-01-01'
{% else %}
where sales_date = date '2020-01-01'
{% endif %}
'''

mode = 'fast'
db = get_db(mode)
df = db.select(sql=sql, jinja_context={'mode': mode})
```

> **NOTE:** If you're worried about cluttering your SQL code with branching logic, keep in mind that Athena and SQLite syntax are nearly identical, so these branches are surprisingly rare.  Also, Jinja offers many tools to help refactor and re-use common Jinja snippets, such as [macros](https://ttl255.com/jinja2-tutorial-part-5-macros) or [global functions](https://stackoverflow.com/questions/6036082/call-a-python-function-from-jinja2), which can be used to reduce clutter.

> **NOTE:**  The `DatabaseConnection` class also provides [`insert`](/aws_utils/database/connection.py#L320) and [`delete`](/aws-utils/blob/master/aws_utils/database/connection.py#L411) methods which behave identically across Glue and SQLite.



## Schema Migrations

Not written yet.  For now, see the [`DatabaseMigrationService`](/aws_utils/database/migration.py) class, [unit test](/tests/database/test_migration.py), and [sample migrations](tests/data/resources/migrations).



## Overcoming Athena Limitations

#### Use Case

AWS Glue and Athena are incredible services, as they provide [separation-of-storage-and-compute](https://ajstorm.medium.com/separating-compute-and-storage-59def4f27d64), serverless and distributed data processing, all with a familiar SQL interface.  That being said, Athena has two big limitations:

1. Athena does not support SQL [`UPDATE`](https://stackoverflow.com/questions/71705848/aws-athena-update-table-rows-using-sql) and [`DELETE`](https://stackoverflow.com/questions/48815504/can-i-delete-data-rows-in-tables-from-athena) commands.  (`ALTER` commands are also limited.)  Basically, if you need to *modify* an *existing* dataset, you have to use another tool.  Proponents will justify this by saying Athena is "only a query service", and while that's technically true, these limitations are a huge blight on Athena's elevator pitch.  The beauty of Athena is that it enables non-experts to wield the power of distributed data processing, a topic that historically has been very difficult to learn, e.g. see Apache Spark's [RDD documentation](https://spark.apache.org/docs/latest/rdd-programming-guide.html).  With the introduction of [`INSERT`](https://docs.aws.amazon.com/athena/latest/ug/insert-into.html) and [CTAS](https://docs.aws.amazon.com/athena/latest/ug/ctas.html) commands, Athena is *so close* to competing as an easier-to-use alternative to Apache Spark, but the inability to modify existing data is a deal-breaker for many projects.  As a workaround, we can try rewriting our `UPDATE` and `DELETE` statements as a clever arrangement of CTAS and `DROP TABLE` statements.  This approach actually works, but gets complicated by the next limitation.

2. Athena `INSERT` and CTAS statements cannot handle more than [100 partitions](https://docs.aws.amazon.com/athena/latest/ug/ctas-insert-into.html).  For example, imagine you have a giant table with >100 partitions, and you simply want to create a backup.  If you try `create table my_backup as select * from my_table`, this command will fail.  Instead, you have to create an empty table, and then loop and insert 100 partitions at a time.  Given that Amazon is already billing us proportional to data volume, I don't understand why they imposed this limitation.  It seems like an unnecessary hurdle.  Also, it forces SQL/DBA types to learn another language (with a `for` loop and an Athena API), which is often outside their expertise.

If you're reading all of this, you might be thinking that my grievances are unreasonable, and that I'm simply using the wrong tool for the job.  You might be right.  In my particular situation, the corporate edict was that all applications read/write to a central data lake on S3.  (Redshift was not permitted.)  Most of the engineering teams were using Apache Spark.  However, my data analytics team was filled with SQL/RDBMS veterans, so I had a choice to make:  Either throw away their decades of RDBMS experience and retrain them in Apache Spark, or *leverage* their valuable skillset and find a way to make Athena SQL work for us.  I chose the latter, which loosely inspired the algorithms below.  Going forward, I became more confident in this decision, because we had an obvious edge in productivity relative to the Spark teams, who were floundering in technical debt, and repeatedly delayed their timelines as they grappled with an unfamiliar and incredibly-complex tech stack.

#### Example

This library introduces a function named [`backup_drop_rebuild`](/aws_utils/database/backup_drop_rebuild.py) to overcome the limitations above.  It can be used to update, delete from, or arbitrarily modify an existing Glue table *and* the underlying data files on S3.  This is best explained with an example.

Suppose we have a Glue table named `job` that looks like this:

```text
| job_id | start_time         | end_time           | status   |
| ------ | ------------------ | ------------------ | -------- |
| 101    | 2022-01-03 4:14 AM | 2022-01-03 4:31 AM | complete |
| 102    | 2022-01-03 6:41 PM | 2022-01-03 6:57 PM | error    |
| 103    | 2022-01-07 3:29 PM | 2022-01-07 3:41 PM | complete |
| …      |                    |                    |          |
| 300    | 2022-01-07 5:01 PM | 2022-01-07 5:14 PM | complete |
```

> **NOTE:**  Assume this table is partitioned by `job_id` and has >100 partitions, thus we cannot create a backup table via an Athena CTAS command.

Now, suppose we wish to add an additional column named `elapsed_time`, which contains the total seconds between `start_time` and `end_time`.  In a traditional RDBMS, we might do something like this:

```sql
alter table job add column elapsed_time int;
update job set elapsed_time = (end_time - start_time) * 24 * 60 * 60;
```

This isn't possible out-of-the-box with Athena SQL.  However, the [`backup_drop_rebuild`](/aws_utils/database/backup_drop_rebuild.py) function provides an elegant interface for achieving the same result:

```python
from pathlib import Path
from aws_utils.database.backup_drop_rebuild import backup_drop_rebuild
from aws_utils.database.connection import DatabaseConnection

db = DatabaseConnection(
    type='glue',
    name='my_db',
    athena_workgroup='my-workgroup',
)

backup_drop_rebuild(
    db=db,
    table_name='job',
    partition_columns=['job_id'],
    create_sql=Path('./create_table.sql'),
    insert_sql=Path('./insert_into.sql'),
    jinja_context={'s3_database_prefix': 's3://my-bucket/path/to/db'},
)
```

This code will perform the following steps:

1. Create a backup table named `job__backup`.
2. Drop the `job` table (and corresponding S3 data).
3. Execute `create_sql` to create a *new* (and empty) `job` table with updated column structure:  [example](/tests/data/resources/migrations/v2/create_table_job.sql).
4. Execute `insert_sql` to insert (and appropriately transform) the backed-up records into the new `job` table:  [example](/tests/data/resources/migrations/v2/insert_into_job.sql).
5. Drop the `job__backup` table (and corresponding S3 data).

> **NOTE:**  For partitioned tables, steps 1 and 4 are processed in chunks, i.e. we loop and
    insert 100 partitions at a time.

So basically, you just need to express your update/delete/alter logic in terms of  `create_sql` and `insert_sql` statements, and then the library will handle everything else.  This approach is both flexible and easy-to-use.

See the [docstrings](/aws_utils/database/backup_drop_rebuild.py#L30) for more information.  Also, see the [unit test](/tests/database/test_migration.py) and [sample migrations](/tests/data/resources/migrations) for a working example.
