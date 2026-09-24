[![Build Status](https://travis-ci.org/keboola/sapi-python-client.svg?branch=master)](https://travis-ci.org/keboola/sapi-python-client)

# Python client for the Keboola Storage API
Client for using [Keboola Connection Storage API](http://docs.keboola.apiary.io/). This API client provides client methods to get data from KBC and store data in KBC. The endpoints 
for working with buckets, tables and workspaces are covered.

## Install

```bash
pip install kbcstorage
```

or 

```bash
pip install git+https://github.com/keboola/sapi-python-client.git
```

## Client Class Usage
```python
from kbcstorage.client import Client, StorageApiToken

client = Client('https://connection.keboola.com', StorageApiToken('your-token'))

# get table data into local file
client.tables.export_to_file(table_id='in.c-demo.some-table', path_name='/data/')

# save data
client.tables.create(name='some-table-2', bucket_id='in.c-demo', file_path='/data/some-table')

# list buckets
client.buckets.list()

# list bucket tables
client.buckets.list_tables('in.c-demo')

# get table info
client.tables.detail('in.c-demo.some-table')

```

## Authentication

The `token` argument takes an authentication strategy, either `StorageApiToken` or `BearerToken`:

```python
from kbcstorage.client import Client, BearerToken, StorageApiToken

# Storage API token, sent as `X-StorageApi-Token`
client = Client('https://connection.keboola.com', StorageApiToken('your-token'))

# Programmatic token, sent as `Authorization: Bearer` with the project id in `X-KBC-ProjectId`
client = Client('https://connection.keboola.com', BearerToken('kbc_at_...', project_id=1234))
```

A bare string is also accepted and is equivalent to `StorageApiToken('your-token')`. It is fully
supported and not deprecated:

```python
client = Client('https://connection.keboola.com', 'your-token')
```

Storage API tokens are bound to a project. Programmatic tokens (`kbc_at_*` session tokens,
`kbc_pat_*` personal access tokens) are not, which is why `BearerToken` requires the project id.

All three forms work for directly constructed endpoints too, and the token is used as given — the
client never refreshes or decodes it, so an expired token surfaces as a `requests.HTTPError` with a
401.

### Breaking change in 0.11.0

A bare string starting with `kbc_at_` or `kbc_pat_` now raises `ValueError` at construction. Before
0.11.0 it was sent as `X-StorageApi-Token`, which the backend rejects with a 401 — the failure moved
from request time to construction time and the message names the fix. Wrap such tokens in
`BearerToken(token, project_id)`.

## Endpoint Classes Usage 
```python
from kbcstorage.auth import StorageApiToken
from kbcstorage.tables import Tables
from kbcstorage.buckets import Buckets

token = StorageApiToken('your-token')

tables = Tables('https://connection.keboola.com', token)

# get table data into local file
tables.export_to_file(table_id='in.c-demo.some-table', path_name='/data/')

# save data
tables.create(name='some-table-2', bucket_id='in.c-demo', file_path='/data/some-table')

# list buckets
buckets = Buckets('https://connection.keboola.com', token)
buckets.list()

# list bucket tables
buckets.list_tables('in.c-demo')

# get table info
tables.detail('in.c-demo.some-table')

```

## Development

### Local environment

The package requires Python 3.10 or newer (`workspaces.load_tables` uses `int | str` annotations); CI builds
and tests on 3.11.

Create a virtual environment and install the package into it in editable mode:

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

`pyproject.toml` declares the test dependencies alongside the runtime ones, and the `dev` extra adds
the linter, so this single command covers everything. The `-e` flag keeps `kbcstorage` resolving to
the working tree, so edits take effect without reinstalling.

Prefixing commands with `.venv/bin/` always uses the environment. Activating it instead makes the
plain `python` and `pip` commands resolve there for the rest of the shell session:

```bash
source .venv/bin/activate
```

On Windows the paths are `.venv\Scripts\python` and `.venv\Scripts\activate`.

### Editor setup

Editors and IDEs resolve imports through a configured interpreter. Until yours is pointed at the
virtual environment, every third-party import is reported as unresolved. The interpreter path is:

```
.venv/bin/python
```

| Editor | Where to set it |
| --- | --- |
| VS Code | `Python: Select Interpreter` |
| PyCharm | Settings → Project → Python Interpreter → Add → Existing environment |
| Zed | `toolchain: select` |
| Vim, Neovim, Emacs | the interpreter or `venv` option of your LSP client |

Most editors scan for a directory named `.venv` or `venv` in the project root, so keeping the default
name avoids manual configuration in the common case. A restart of the language server is often
needed before existing errors clear.

### Checks

Lint. CI runs this before the tests, so a failure stops the build before anything is executed:

```bash
.venv/bin/python -m flake8
```

Unit tests, which mock all API responses and need no credentials:

```bash
.venv/bin/python -m unittest discover -s tests/mocks -t .
```

The functional tests run against a real project and are covered below.

## Tests
Create `.env` file according to the `.env.template` file and run the tests with:

```bash
$ docker compose run --rm -e KBC_TEST_TOKEN -e KBC_TEST_API_URL ci -m unittest discover
```

## Contribution Guide
The client is far from supporting the entire API, all contributions are very welcome. New API endpoints should 
be implemented in their own class extending `Endpoint`. Naming conventions should follow existing naming conventions
or those of the [API](http://docs.keboola.apiary.io/#). If the method contains some processing of the request or 
response, consult the corresponding [PHP implementation](https://github.com/keboola/storage-api-php-client) for 
reference. New code should be covered by tests.

Note that if you submit a PR from your own forked repository, the automated functional tests will fail. 
This is expected for security reasons, please do send the PR anyway. 
Either run the tests locally (set `KBC_TEST_TOKEN` (your token to test project) and 
`KBC_TEST_API_URL` (https://connection.keboola.com) variables) or ask for access. In case, you need a 
project for local testing, feel free to [ask for one](https://developers.keboola.com/#development-project).

The recommended workflow for making a pull request is:

```bash
git clone https://github.com/keboola/sapi-python-client.git
git checkout master
git pull
git checkout -b my-new-feature
# work on branch my-new-feature
git push origin my-new-feature:my-new-feature
```

This will create a new branch which can be used to make a pull request for your new feature.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
