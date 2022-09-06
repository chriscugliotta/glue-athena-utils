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

Both methods are (optionally) augmented via [Jinja2](https://jinja.palletsprojects.com/en/3.1.x/) to enable easy and expressive query parameterization.  Sometimes this approach is called "Jinja SQL."

For example, suppose we have a multi-tenant database, e.g. a single table contains data for multiple clients.  Suppose we're trying to load training data (for an ML model) across the clients.  However, imagine that each client has different data availability, and thus the query needs to be slightly customized for each client.  Here is an example of how Jinja SQL can help.

```python
from aws_utils.database.connection import DatabaseConnection

db = DatabaseConnection(
    type='glue',
    s3_database_prefix='s3://my-bucket/path/to/db',
    athena_workgroup='my-workgroup',
)

params = {
    'schema': 'my_schema',
    'clients': [
        ('client1', '2020-01-01', '2020-12-31'),
        ('client2', '2020-01-01', '2020-09-31'),
        ('client3', '2021-01-01', '2021-12-31'),
    ]
}

sql = '''
select *
from {{schema}}.my_table
where 1 = 1
    {% for client, min_date, max_date in clients %}
    and (client = '{{client}}' and sales_date between '{{min_date}}' and '{{max_date}}'){{ ' or' if not loop.last }}
    {% endfor %}
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
where 1 = 1
    and (client = 'client1' and sales_date between '2020-01-01' and '2020-12-31') or
    and (client = 'client2' and sales_date between '2020-01-01' and '2020-09-30') or
    and (client = 'client3' and sales_date between '2021-01-01' and '2021-12-31')
```

Admittedly, the Jinja template syntax is pretty ugly.  However, the improved expressivity is often worth the tradeoff.  It allows your queries to change dynamically with respect to runtime parameters.  See the [Query Mocking](#query-mocking) section for a particularly powerful example.

> **NOTE:** You can use something like [J2Live](https://j2live.ttl255.com) to quickly validate template syntax.



## Query Caching

TODO.
