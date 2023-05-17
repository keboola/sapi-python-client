import csv
import os
import tempfile
import warnings
from requests import exceptions
from kbcstorage.buckets import Buckets
from kbcstorage.tables import Tables
from tests.base_test_case import BaseTestCase


class TestEndpoint(BaseTestCase):
    def setUp(self):
        self.buckets = Buckets(os.getenv('KBC_TEST_API_URL'),
                               os.getenv('KBC_TEST_TOKEN'))
        try:
            self.buckets.delete('in.c-py-test-buckets', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

        # https://github.com/boto/boto3/issues/454
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            self.buckets.delete('in.c-py-test-buckets', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_create_bucket(self):
        bucket_id = self.buckets.create(name='py-test-buckets',
                                        stage='in',
                                        description='Test bucket')['id']
        self.assertEqual(bucket_id, self.buckets.detail(bucket_id)['id'])

    def test_list_tables(self):
        bucket_id = self.buckets.create(name='py-test-buckets',
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
                      bucket_id='in.c-py-test-buckets')
        tables = self.buckets.list_tables(bucket_id)
        with self.subTest():
            self.assertEqual(1, len(tables))
        with self.subTest():
            self.assertEqual('in.c-py-test-buckets.some-table',
                             tables[0]['id'])

    def test_bucket_detail(self):
        bucket_id = self.buckets.create(name='py-test-buckets',
                                        stage='in',
                                        description='Test bucket')['id']
        detail = self.buckets.detail(bucket_id)
        with self.subTest():
            self.assertEqual(bucket_id, detail['id'])
        with self.subTest():
            self.assertEqual('c-py-test-buckets', detail['name'])
        with self.subTest():
            self.assertIsNotNone(detail['uri'])
        with self.subTest():
            self.assertIsNotNone(detail['created'])
        with self.subTest():
            self.assertEqual('Test bucket', detail['description'])
        with self.subTest():
            self.assertEqual([], detail['tables'])
        with self.subTest():
            self.assertEqual([], detail['attributes'])

    def test_invalid_bucket(self):
        try:
            self.buckets.detail('some-totally-non-existent-bucket')
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
