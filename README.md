# Python client for the Keboola Storage API

## Install

`$ pip install git+https://github.com/keboola/sapi-python-client.git`

or 

```bash
$ git clone https://github.com/keboola/sapi-python-client.git && cd sapi-python-client
$ python setup.py install
```

## Usage 
```
from kbcstorage.client import Client

cl = Client("MY_KBC_TOKEN")

# get table data into local file
cl.get_table_data("in.c-myBucket.myTable", "local_file_name.csv')

# save data
cl.save_table("tableName", "in.c-myBucket", "csv_I_want_to_store.csv")

# list buckets
cl.list_buckets()

# list bucket tables
cl.list_tables(bucketId)

# get table info
cl.get_table(tableId)

```

Under development -- all contributions very welcome :)

Kickstarted via https://gist.github.com/Halama/6006960 
.
