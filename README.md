# ThanoSQL Python Library

The ThanoSQL Python library provides convenient access to the ThanoSQL API from any Python 3.8+ application. The library covers all ThanoSQL operations that users can do, such as querying, editing tables, and much more.

## Installation

Refer to the following instructions to install the ThanoSQL Python library from PyPI.

```bash
pip install thanosql
```

> [!WARNING]
> Make sure to uninstall ThanoSQL Magic first.
>
> `pip uninstall thanosql-magic`
>
> In order to use the library with ThanoSQL Magic,
>
> `pip install 'thanosql[magic]'`

Alternatively, you can also install from source. First, clone this repository.

```bash
git clone https://github.com/smartmind-team/thanosql-sdk-python.git
```

Install the SDK by using `pip install`. Note that the Python version used during development is 3.8.10.

```bash
pip install -e .
pip install -e ."[dev]" # include unit test
pip install -e ."[magic]" # include magic
```

## Usage

In order to use the library, a working workspace engine is required. Create a new Python or IPython notebook file. Import the `thanosql` package, create a `ThanoSQL` client with your API token and engine URL, and then you can use all the functions in the library. For more examples, head over to the [examples/](./examples/) directory.

1. Set up your API_TOKEN and ENGINE_URL (recommended).

   ```bash
   export THANOSQL_API_TOKEN='your-api-token-here'
   export THANOSQL_ENGINE_URL='your-engine-url-here'
   ```

2. Import the ThanoSQL client and use it to query your ThanoSQL Workspace.

   ```python
   from thanosql import ThanoSQL

   client = ThanoSQL()
   # defaults to getting the token using os.environ.get("THANOSQL_API_TOKEN"),
   # and also defaults to getting the url using os.environ.get("THANOSQL_ENGINE_URL"),
   # client = ThanoSQL(
   #     api_token='your-api-token-here',
   #     engine_url='your-engine-url-here'
   # )

   res = client.query.execute(query="SELECT 1")
   print(res.model_dump_json(indent=4))

   tables = client.table.list(schema="public")

   # do something with the list of tables
   for table in tables:
      print(table.name)
   ```

## Magic

`thanosql-magic` is a Jupyter Notebook extension that provides SQL query capabilities using [ThanoSQL](https://www.thanosql.ai). This magic extension enables users to interact with ThanoSQL Workspace databases using extended SQL syntax within a Jupyter notebook.

`thanosql-magic` uses IPython magic. [IPython magic](https://ipython.readthedocs.io/en/stable/interactive/magics.html) is a special command that can be used in the IPython shell to perform specific tasks before executing the code. Since Jupyter includes the IPython shell, you can also use these magic commands in Jupyter Notebook.

IPython magic commands are prefixed with % or %% and % applies the magic to a single line of code, while %% applies the magic to multiple lines of code.

### Installation

To install thanosql-magic, you can use pip:

```bash
pip install 'thanosql[magic]'
```

Once installed, you can load the extension in your Jupyter notebook by running:

```python
%load_ext thanosql
```

### Usage

After loading the extension, you can connect to your ThanoSQL Engine instance by setting the thanosql variable:

1. Setting API_TOKEN

   ```python
   %thanosql API_TOKEN=<Issued_API_TOKEN>
   ```

2. Changing the Default API URI (Optional)

   ```python
   %thanosql http://localhost:8000/api/v1/query
   ```

3. Using Magic Commands

   You can then execute SQL queries on your Thanos data using the %thanosql magic command:

   ```python
   %%thanosql
   SELECT * FROM users
   ```

   This will run the SQL query and display the results in your Jupyter notebook.
