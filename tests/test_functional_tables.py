import os
import unittest
import tempfile
import csv
from requests import exceptions
from kbcstorage.tables import Tables
from kbcstorage.buckets import Buckets


class TestFunctionalBuckets(unittest.TestCase):
    def setUp(self):
        self.tables = Tables(os.getenv('KBC_TEST_API_URL') + '/v2/storage/',
                             os.getenv('KBC_TEST_TOKEN'))
        self.buckets = Buckets(os.getenv('KBC_TEST_API_URL') + '/v2/storage/',
                               os.getenv('KBC_TEST_TOKEN'))
        try:
            self.buckets.delete('in.c-py-test', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        self.buckets.create(name='py-test', stage='in')

    def tearDown(self):
        try:
            self.buckets.delete('in.c-py-test', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_create_table_minimal(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'], lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'ping', 'col2': 'pong'})
        table_id = self.tables.create(name='some-table', file_path=path, bucket_id='in.c-py-test')
        os.close(file)
        table_info = self.tables.detail(table_id)
        print(table_info)
        self.assertEqual(table_id, table_info['id'])
        self.assertEqual('in.c-py-test', table_info['bucket'])

    def test_table_detail(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'], lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'ping', 'col2': 'pong'})
        table_id = self.tables.create(name='some-table', file_path=path, bucket_id='in.c-py-test')
        table_info = self.tables.detail(table_id)
        self.assertTrue('created' in table_info)

    def test_invalid_create(self):
        try:
            self.tables.detail('some-totally-non-existent-table')
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
