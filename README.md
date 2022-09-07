# aws-utils

Productivity tools for Amazon Glue and Athena.

This library provides a class named [`DatabaseConnection`](aws_utils/database/connection.py), which provides a common interface to many database types, e.g. Glue, SQLite, Oracle, etc.  This isn't an ORM.  It is a lightweight database abstraction layer that focuses on convenience and productivity.  **The target audience is people who prefer SQL over OOP**, in the context of tabular data processing.



# Contents

- [Query Parameterization](#query-parameterization)
- [Query Caching](#query-caching)
- [Query Mocking](#query-mocking)
- [Schema Migrations](#schema-migrations)
- [Overcoming Athena Limitations](#overcoming-athena-limitations)



## Query Parameterization

The `DatabaseConnection` class has two primary methods:  [`select`](/aws_utils/database/connection.py#L136) and [`execute`](/aws_utils/database/connection.py#L256).

- `select`:  Executes a SQL statement, and returns the query result as a Pandas dataframe.
- `execute`:  Executes arbitrary SQL on the database, e.g. inserts, DDL commands, etc.

Both methods are (optionally) augmented via [Jinja2](https://jinja.palletsprojects.com/en/3.1.x/) to enable easy and expressive query parameterization.  Sometimes this approach is called "Jinja SQL."  Here is an example:

```python
from aws_utils.database.connection import DatabaseConnection

db = DatabaseConnection(
    type='glue',
    s3_database_prefix='s3://my-bucket/path/to/db',
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

Admittedly, the Jinja template syntax is pretty ugly.  However, the improved expressivity is often worth the tradeoff.

> **NOTE:** You can use something like [J2Live](https://j2live.ttl255.com) to quickly validate the template syntax.

For example, suppose you maintain a standard SQL script, and suddenly need to accommodate a client customization, e.g. a loading filter that is only applicable to a single client.  In this case, consider modifying your standard script, i.e. use a Jinja `if` statement to conditionally inject the SQL `where` clause.  Perhaps this is ugly, but client customizations are always ugly, and this approach is a lot cleaner than forking your codebase for each client.
