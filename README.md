# aws-utils

This library aims to improve developer experience (DX) and productivity with AWS Glue and Athena.  It contains several utilities that help streamline a Python/SQL workflow, with a focus on fast automated testing, automated schema migrations, and overcoming AWS Athena's [update](https://stackoverflow.com/questions/71705848/aws-athena-update-table-rows-using-sql) and [100-partition](https://docs.aws.amazon.com/athena/latest/ug/ctas-insert-into.html) limitations.  **The target audience is people who prefer SQL over OOP** in the context of tabular data processing.



# Contents

- [Getting Started](#getting-started)
- [Query Parameterization](#query-parameterization)
    - [Use Case](#use-case)
    - [Example](#example)
- [Query Testing](#query-testing)
    - [Use Case](#use-case-1)
    - [Example](#example-1)
- [Schema Migrations](#schema-migrations)
    - [Use Case](#use-case-2)
    - [Example](#example-2)
- [Overcoming Athena Limitations](#overcoming-athena-limitations)
    - [Use Case](#use-case-3)
    - [Example](#example-3)



## Getting Started

This library can be installed via pip:

```
pip install git+https://github.com/chriscugliotta/aws-utils.git
```

This library introduces a class named [`DatabaseConnection`](aws_utils/database/connection.py) which has two primary methods:

-  [`select`](/aws_utils/database/connection.py#L136):  Executes a SQL statement and returns the query result as a Pandas dataframe.
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

Not written yet.  For now, see the [`backup_drop_rebuild`](/aws_utils/database/backup_drop_rebuild.py) module.
