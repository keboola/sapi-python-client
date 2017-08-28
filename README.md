[![Build Status](https://travis-ci.org/keboola/psapi-python-client.svg?branch=master)](https://travis-ci.org/keboola/sapi-python-client)

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

## Usage 
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
tables.detail('in.c-demo')

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

Under development -- all contributions very welcome :)

Kickstarted via https://gist.github.com/Halama/6006960 
