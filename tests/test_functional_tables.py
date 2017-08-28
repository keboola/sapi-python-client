import os
import unittest
import tempfile
import csv
import warnings
from requests import exceptions
from kbcstorage.tables import Tables
from kbcstorage.buckets import Buckets


class TestFunctionalBuckets(unittest.TestCase):
    def setUp(self):
        self.tables = Tables(os.getenv('KBC_TEST_API_URL'),
                             os.getenv('KBC_TEST_TOKEN'))
        self.buckets = Buckets(os.getenv('KBC_TEST_API_URL'),
                               os.getenv('KBC_TEST_TOKEN'))
        try:
            self.buckets.delete('in.c-py-test', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        self.buckets.create(name='py-test', stage='in')
        # https://github.com/boto/boto3/issues/454
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            self.buckets.delete('in.c-py-test', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_create_table_minimal(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'ping', 'col2': 'pong'})
        os.close(file)
        table_id = self.tables.create(name='some-table', file_path=path,
                                      bucket_id='in.c-py-test')
        table_info = self.tables.detail(table_id)
        self.assertEqual(table_id, table_info['id'])
        self.assertEqual('in.c-py-test', table_info['bucket']['id'])

    def test_table_detail(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'ping', 'col2': 'pong'})
        table_id = self.tables.create(name='some-table', file_path=path,
                                      bucket_id='in.c-py-test')
        table_info = self.tables.detail(table_id)
        self.assertEqual(table_id, table_info['id'])
        self.assertEqual('some-table', table_info['name'])
        self.assertEqual('https://connection.keboola.com/v2/storage/tables/in'
                         '.c-py-test.some-table', table_info['uri'])
        self.assertEqual([], table_info['primaryKey'])
        self.assertEqual([], table_info['indexedColumns'])
        self.assertEqual(['col1', 'col2'], table_info['columns'])
        self.assertTrue('created' in table_info)
        self.assertTrue('lastImportDate' in table_info)
        self.assertTrue('lastChangeDate' in table_info)
        self.assertTrue('rowsCount' in table_info)
        self.assertTrue('metadata' in table_info)
        self.assertTrue('bucket' in table_info)
        self.assertTrue('columnMetadata' in table_info)

    def test_delete_table(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'ping', 'col2': 'pong'})
        table_id = self.tables.create(name='some-table', file_path=path,
                                      bucket_id='in.c-py-test')
        table_info = self.tables.detail(table_id)
        self.assertEqual(table_id, table_info['id'])
        self.tables.delete(table_id)
        try:
            self.tables.detail('some-totally-non-existent-table')
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_invalid_create(self):
        try:
            self.tables.detail('some-totally-non-existent-table')
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_import_table_incremental(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'ping', 'col2': 'pong'})
        os.close(file)
        table_id = self.tables.create(name='some-table', file_path=path,
                                      bucket_id='in.c-py-test')
        table_info = self.tables.detail(table_id)
        self.assertEqual(table_id, table_info['id'])
        self.assertEqual(1, table_info['rowsCount'])

        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'foo', 'col2': 'bar'})
        os.close(file)
        self.tables.load(table_id=table_id, file_path=path,
                         is_incremental=True)
        table_info = self.tables.detail(table_id)
        self.assertEqual(table_id, table_info['id'])
        self.assertEqual(2, table_info['rowsCount'])

    def test_import_table_no_incremental(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'ping', 'col2': 'pong'})
        os.close(file)
        table_id = self.tables.create(name='some-table', file_path=path,
                                      bucket_id='in.c-py-test')
        table_info = self.tables.detail(table_id)
        self.assertEqual(table_id, table_info['id'])
        self.assertEqual(1, table_info['rowsCount'])

        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'foo', 'col2': 'bar'})
        os.close(file)
        self.tables.load(table_id=table_id, file_path=path,
                         is_incremental=False)
        table_info = self.tables.detail(table_id)
        self.assertEqual(table_id, table_info['id'])
        self.assertEqual(1, table_info['rowsCount'])

    def test_table_preview(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'ping', 'col2': 'pong'})
            writer.writerow({'col1': 'foo', 'col2': 'bar'})
        os.close(file)
        table_id = self.tables.create(name='some-table', file_path=path,
                                      bucket_id='in.c-py-test')
        contents = self.tables.preview(table_id=table_id)
        print(contents)
        lines = contents.split('\n')
        self.assertEqual(['"col1","col2"', '"foo","bar"', '"ping","pong"'],
                         lines)

    def test_table_export(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'ping', 'col2': 'pong'})
            writer.writerow({'col1': 'foo', 'col2': 'bar'})
        os.close(file)
        table_id = self.tables.create(name='some-table', file_path=path,
                                      bucket_id='in.c-py-test')
        result = self.tables.export(table_id=table_id)
        self.assertTrue('file' in result)
        self.assertTrue('id' in result['file'])
