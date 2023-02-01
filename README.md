[![Build Status](https://travis-ci.org/keboola/sapi-python-client.svg?branch=master)](https://travis-ci.org/keboola/sapi-python-client)

# Python client for the Keboola Storage API
Client for using [Keboola Connection Storage API](http://docs.keboola.apiary.io/). This API client provides client methods to get data from KBC and store data in KBC. The endpoints 
for working with buckets, tables and workspaces are covered.

## Install

`$ pip3 install git+https://github.com/keboola/sapi-python-client.git`

or 

```bash
$ git clone https://github.com/keboola/sapi-python-client.git && cd sapi-python-client
$ python setup.py install
```

## Client Class Usage
```
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
```
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

## Docker image
Docker image with pre-installed library is also available, run it via:

```
docker run -i -t quay.io/keboola/sapi-python-client
```

## Tests

```bash
$ git clone https://github.com/keboola/sapi-python-client.git && cd sapi-python-client
$ python setup.py test
```

or 

```bash
$ docker-compose run --rm -e KBC_TEST_TOKEN -e KBC_TEST_API_URL sapi-python-client -m unittest discover
```

## Contribution Guide
The client is far from supporting the entire API, all contributions are very welcome. New API endpoints should 
be implemeneted in their own class extending `Endpoint`. Naming conventions should follow existing naming conventions
or those of the [API](http://docs.keboola.apiary.io/#). If the method contains some processing of the request or response, consult the corresponing [PHP implementation](https://github.com/keboola/storage-api-php-client) for reference. New code should be covered by tests.

Note that if you submit a PR from your own forked repository, the automated functional tests will fail. This is limitation of [Travis](https://docs.travis-ci.com/user/pull-requests/#Pull-Requests-and-Security-Restrictions). Either run the tests locally (set `KBC_TEST_TOKEN` (your token to test project) and `KBC_TEST_API_URL` (https://connection.keboola.com) variables) or ask for access. In case, you need a project for local testing, feel free to [ask for one](https://developers.keboola.com/#development-project).

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
