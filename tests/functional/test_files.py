import csv
import os
import tempfile
import warnings
import time
from requests import exceptions
from kbcstorage.buckets import Buckets
from kbcstorage.files import Files
from kbcstorage.tables import Tables
from tests.base_test_case import BaseTestCase


class TestEndpoint(BaseTestCase):
    def setUp(self):
        # timeout for files from previous tests to appear
        time.sleep(1)
        self.files = Files(os.getenv('KBC_TEST_API_URL'),
                           os.getenv('KBC_TEST_TOKEN'))
        files = self.files.list(tags=['py-test'])
        for file in files:
            self.files.delete(file['id'])
        # https://github.com/boto/boto3/issues/454
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        # timeout for files from previous tests to appear
        time.sleep(1)
        files = self.files.list(tags=['py-test'])
        for file in files:
            self.files.delete(file['id'])

    def test_create_file(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        os.write(file, bytes('fooBar', 'utf-8'))
        file_id = self.files.upload_file(path, tags=['py-test', 'file1'])
        os.close(file)
        file_info = self.files.detail(file_id)
        with self.subTest():
            self.assertEqual(file_id, file_info['id'])
        with self.subTest():
            self.assertEqual(6, file_info['sizeBytes'])
        with self.subTest():
            self.assertFalse(file_info['isPublic'])
        with self.subTest():
            self.assertFalse(file_info['isSliced'])
        with self.subTest():
            self.assertTrue(file_info['isEncrypted'])
        with self.subTest():
            self.assertTrue('name' in file_info)
        with self.subTest():
            self.assertTrue('created' in file_info)
        with self.subTest():
            self.assertTrue('url' in file_info)
        with self.subTest():
            self.assertTrue('region' in file_info)
        with self.subTest():
            self.assertTrue('creatorToken' in file_info)
        with self.subTest():
            self.assertTrue('tags' in file_info)
        with self.subTest():
            self.assertTrue('py-test' in file_info['tags'])
        with self.subTest():
            self.assertTrue('file1' in file_info['tags'])
        with self.subTest():
            self.assertFalse('credentials' in file_info)

    def test_create_file_compress(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        os.write(file, bytes('fooBar', 'utf-8'))
        file_id = self.files.upload_file(path, tags=['py-test', 'file1'],
                                         compress=True)
        os.close(file)
        time.sleep(1)
        file_info = self.files.detail(file_id)
        with self.subTest():
            self.assertEqual(file_id, file_info['id'])

    def test_delete_file(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        os.write(file, bytes('fooBar', 'utf-8'))
        file_id = self.files.upload_file(path, tags=['py-test', 'file1'])
        os.close(file)
        time.sleep(1)
        with self.subTest():
            self.assertEqual(file_id, self.files.detail(file_id)['id'])
        self.files.delete(file_id)
        try:
            self.assertEqual(file_id, self.files.detail(file_id)['id'])
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_download_file_credentials(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        os.write(file, bytes('fooBar', 'utf-8'))
        file_id = self.files.upload_file(path, tags=['py-test', 'file1'])
        os.close(file)
        time.sleep(1)
        file_info = self.files.detail(file_id, federation_token=True)
        with self.subTest():
            self.assertEqual(file_id, file_info['id'])
        with self.subTest():
            self.assertTrue(file_info['provider'] in ['aws', 'azure', 'gcp'])
        if file_info['provider'] == 'aws':
            with self.subTest():
                self.assertTrue('credentials' in file_info)
            with self.subTest():
                self.assertTrue('AccessKeyId' in file_info['credentials'])
            with self.subTest():
                self.assertTrue('SecretAccessKey' in file_info['credentials'])
            with self.subTest():
                self.assertTrue('SessionToken' in file_info['credentials'])
        elif file_info['provider'] == 'azure':
            with self.subTest():
                self.assertTrue('absCredentials' in file_info)
            with self.subTest():
                self.assertTrue('SASConnectionString' in file_info['absCredentials'])
            with self.subTest():
                self.assertTrue('expiration' in file_info['absCredentials'])
            with self.subTest():
                self.assertTrue('absPath' in file_info)
            with self.subTest():
                self.assertTrue('container' in file_info['absPath'])
            with self.subTest():
                self.assertTrue('name' in file_info['absPath'])

    def test_download_file(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        os.write(file, bytes('fooBar', 'utf-8'))
        file_id = self.files.upload_file(path, tags=['py-test', 'file1'])
        os.close(file)
        tmp = tempfile.TemporaryDirectory()
        local_path = self.files.download(file_id, tmp.name)
        with open(local_path, mode='rb') as file:
            data = file.read()
        self.assertEqual('fooBar', data.decode('utf-8'))

    def test_download_file_sliced(self):
        buckets = Buckets(os.getenv('KBC_TEST_API_URL'),
                          os.getenv('KBC_TEST_TOKEN'))
        try:
            buckets.delete('in.c-py-test-files', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        buckets.create(name='py-test-files', stage='in')

        tables = Tables(os.getenv('KBC_TEST_API_URL'),
                        os.getenv('KBC_TEST_TOKEN'))
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'ping', 'col2': 'pong'})
        os.close(file)
        table_id = tables.create(name='some-table', file_path=path,
                                      bucket_id='in.c-py-test-files')
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',',
                                    quotechar='"')
            writer.writeheader()
            writer.writerow({'col1': 'foo', 'col2': 'bar'})
        os.close(file)
        tables.load(table_id=table_id, file_path=path,
                    is_incremental=True)
        file_id = tables.export(table_id=table_id)
        temp_path = tempfile.TemporaryDirectory()
        local_path = self.files.download(file_id, temp_path.name)
        with open(local_path, mode='rt') as file:
            lines = file.readlines()
        self.assertEqual(['"foo","bar"\n', '"ping","pong"\n'], sorted(lines))
