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
from kbcstorage.client import Client

client = Client('https://connection.keboola.com', 'your-token')

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

## Endpoint Classes Usage 
```python
from kbcstorage.tables import Tables
from kbcstorage.buckets import Buckets

tables = Tables('https://connection.keboola.com', 'your-token')

# get table data into local file
tables.export_to_file(table_id='in.c-demo.some-table', path_name='/data/')

# save data
tables.create(name='some-table-2', bucket_id='in.c-demo', file_path='/data/some-table')

# list buckets
buckets = Buckets('https://connection.keboola.com', 'your-token')
buckets.list()

# list bucket tables
buckets.list_tables('in.c-demo')

# get table info
tables.detail('in.c-demo.some-table')

```

## Tests
Create `.env` file according to the `.env.template` file and run the tests with:

```bash
$ docker compose run --rm -e KBC_TEST_TOKEN -e KBC_TEST_API_URL sapi-python-client -m unittest discover
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
