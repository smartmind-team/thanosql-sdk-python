# ThanoSQL Python Library

The ThanoSQL Python library provides convenient access to the ThanoSQL API from any Python 3.8+ application. The library covers all ThanoSQL operations that users can do, such as querying, editing tables, and much more.

## Usage

In order to use the library, first clone this repository.

```bash
git clone https://github.com/smartmind-team/thanosql-sdk-python.git
```

Before starting with the SDK, install all the requirements. While not necessary, using a virtual environment, such as `virtualenv` or `venv` is highly recommended to avoid package conflicts. Note that the Python version used during development is 3.8.16.

```bash
python -m venv {path_to_virtual_environment}
source {path_to_virtual_environment}/bin/activate
pip install -r requirements.txt  # use requirements-test.txt instead if you want to develop unit tests
```

Next, configure the required environment variables. A working workspace engine is required. If you are running a Python script through a terminal, use `export`.

```bash
export THANOSQL_API_TOKEN={your_engine_api_token}
export THANOSQL_ENGINE_URL={your_engine_url}
```

In the root directory of the repository, create a new Python or IPython notebook file, and optionally add the filename to `.gitignore`. Files beginning with `trial` are ignored by default. For practical purposes, assume that a Python file called `trial.py` is used. Import the `thanosql` package, create a `ThanoSQL` client, and you can use all the functions in the library. In the example below, we will use the library to show all tables in the workspace. For more examples, head over to the `examples/` directory.

```python
# trial.py

from thanosql import ThanoSQL

client = ThanoSQL(api_token=THANOSQL_API_VERSION, engine_url=THANOSQL_ENGINE_URL)

res = client.table.list()

# do something with res
print(res.json())

```

Available methods will be further described in the next section. `*` indicates a required parameter.

## Methods

### Query, Query Log, and Query Template APIs

```python
# Running a ThanoSQL or PSQL query
client.query.execute(query_type, query, template_id, template_name, parameters, schema, table_name, overwrite, max_results)

# Showing stored query logs
client.query.log.list(search, offset, limit)

# Showing stored query templates
client.query.template.list(search, offset, limit, order_by)

# Creating a query template
client.query.template.create(name, query, dry_run)

# Showing the details of a query template
client.query.template.get(name*)

# Updating a query template
client.query.template.update(current_name*, new_name, query)

# Deleting a query template
client.query.template.delete(name*)
```

### Table and Table Template APIs

```python
# Showing stored tables
client.table.list(schema, verbose, offset, limit)

# Showing the details of a table
client.table.get(name*, schema)

# Creating a table
client.table.create(name*, schema, table)

# Updating a table
client.table.update(name*, schema, table)

# Uploading a table from a CSV or Excel file
client.table.upload(name*, file*, schema, table, if_exists)

# Deleting a table
client.table.delete(name*, schema)

# Showing stored table templates
client.table.template.list(search, order_by, latest)

# Showing the details of one or more table templates of a certain name
client.table.template.get(name*, version)

# Creating a table template
client.table.template.create(name*, table_template*, version, compatibility)

# Deleting one or more table templates of a certain name
client.table.template.delete(name*, version)
```

In order to create a table or table template object, some classes need to be imported in addition to the client. Refer to the example for table and table template APIs for more detail. `client.table.get()` return a `Table` object, which is required to access the record entries of a certain table.

```python
my_table = client.table.get(name=my_table_name)

# Showing the entries of a table
my_table.get_records(offset, limit)

# Saving the entries of a table into a CSV file
my_table.get_records_as_csv(timezone_offset)

# Inserting new entry(ies) into a table
my_table.insert(records)
```

### View APIs

```python
# Showing stored views
client.view.list(schema, verbose, offset, limit)

# Showing the details of a view
client.view.get(name*, schema)

# Deleting a view
client.view.delete(name*, schema)
```

### Schema APIs

```python
# Showing stored schemas
client.schema.list()

# Creating a new schema
client.schema.create(name*)
```

### File APIs

```python
# Showing the contents of a directory
client.file.list(path*)

# Uploading a file to the workspace
client.file.upload(path*, db_commit, table, column, dir)

# Deleting a file from the workspace
client.file.delete(path*, db_commit, table, column)
```
