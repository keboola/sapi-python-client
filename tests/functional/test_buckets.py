import csv
import os
import tempfile
import unittest
import warnings

from requests import exceptions
from kbcstorage.buckets import Buckets
from kbcstorage.tables import Tables


class TestFunctionalBuckets(unittest.TestCase):
    def setUp(self):
        self.buckets = Buckets(os.getenv('KBC_TEST_API_URL'),
                               os.getenv('KBC_TEST_TOKEN'))
        try:
            self.buckets.delete('in.c-py-test', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        # https://github.com/boto/boto3/issues/454
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            self.buckets.delete('in.c-py-test', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_create_bucket(self):
        bucket_id = self.buckets.create(name='py-test',
                                        stage='in',
                                        description='Test bucket')['id']
        self.assertEqual(bucket_id, self.buckets.detail(bucket_id)['id'])

    def test_list_tables(self):
        bucket_id = self.buckets.create(name='py-test',
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
        tables = Tables(os.getenv('KBC_TEST_API_URL'),
                        os.getenv('KBC_TEST_TOKEN'))
        tables.create(name='some-table', file_path=path,
                      bucket_id='in.c-py-test')
        tables = self.buckets.list_tables(bucket_id)
        self.assertEqual(1, len(tables))
        self.assertEqual('in.c-py-test.some-table', tables[0]['id'])

    def test_bucket_detail(self):
        bucket_id = self.buckets.create(name='py-test',
                                        stage='in',
                                        description='Test bucket')['id']
        detail = self.buckets.detail(bucket_id)
        self.assertEqual(bucket_id, detail['id'])
        self.assertEqual('c-py-test', detail['name'])
        self.assertIsNotNone(detail['uri'])
        self.assertIsNotNone(detail['created'])
        self.assertEqual('Test bucket', detail['description'])
        self.assertEqual([], detail['tables'])
        self.assertEqual([], detail['attributes'])

    def test_invalid_bucket(self):
        try:
            self.buckets.detail('some-totally-non-existent-bucket')
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
