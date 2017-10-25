import csv
import os
import tempfile
import unittest
import warnings

from requests import exceptions
from kbcstorage.client import Client


class TestBuckets(unittest.TestCase):
    def setUp(self):
        self.client = Client(os.getenv('KBC_TEST_API_URL'),
                             os.getenv('KBC_TEST_TOKEN'))
        try:
            self.client.buckets.delete('in.c-py-test', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        # https://github.com/boto/boto3/issues/454
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            self.client.buckets.delete('in.c-py-test', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_client(self):
        bucket_id = self.client.buckets.create(name='py-test',
                                               stage='in',
                                               description='Test bucket')['id']
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'ping', 'col2': 'pong'})
        os.close(file)
        self.assertEqual(bucket_id,
                         self.client.buckets.detail(bucket_id)['id'])
        table_id = self.client.tables.create(name='some-table', file_path=path,
                                             bucket_id='in.c-py-test')
        table_info = self.client.tables.detail(table_id)
        self.assertEqual(table_id, table_info['id'])
        self.assertEqual('in.c-py-test', table_info['bucket']['id'])
        self.assertTrue(len(self.client.jobs.list()) > 2)
        self.assertEqual(1, len(self.client.files.list(limit=1)))
