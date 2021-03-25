import csv
import os
import tempfile
import unittest
import warnings

from azure.storage.blob import BlobServiceClient
from requests import exceptions
from kbcstorage.buckets import Buckets
from kbcstorage.jobs import Jobs
from kbcstorage.files import Files
from kbcstorage.tables import Tables
from kbcstorage.workspaces import Workspaces


class TestWorkspaces(unittest.TestCase):
    def setUp(self):
        self.workspaces = Workspaces(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        self.buckets = Buckets(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        self.jobs = Jobs(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        self.tables = Tables(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        self.files = Files(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        try:
            file_list = self.files.list(tags=['sapi-client-python-tests'])
            for file in file_list:
                self.files.delete(file['id'])
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        try:
            self.buckets.delete('in.c-py-test-buckets', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        # https://github.com/boto/boto3/issues/454
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            if hasattr(self, 'workspace_id'):
                self.workspaces.delete(self.workspace_id)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        try:
            self.buckets.delete('in.c-py-test-tables', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_create_workspace(self):
        workspace = self.workspaces.create()
        self.workspace_id = workspace['id']
        with self.subTest():
            self.assertTrue('id' in workspace)
        with self.subTest():
            self.assertTrue('type' in workspace)
            self.assertTrue(workspace['type'] in ['table', 'file'])
        with self.subTest():
            self.assertTrue('name' in workspace)
        with self.subTest():
            self.assertTrue('component' in workspace)
        with self.subTest():
            self.assertTrue('configurationId' in workspace)
        with self.subTest():
            self.assertTrue('created' in workspace)
        with self.subTest():
            self.assertTrue('connection' in workspace)
        with self.subTest():
            self.assertTrue('backend' in workspace['connection'])
        with self.subTest():
            self.assertTrue('creatorToken' in workspace)

    def test_load_tables_to_workspace(self):
        bucket_id = self.buckets.create('py-test-tables')['id']
        table1_id = self.__create_table(bucket_id, 'test-table-1', {'col1': 'ping', 'col2': 'pong'})
        table2_id = self.__create_table(bucket_id, 'test-table-2', {'col1': 'king', 'col2': 'kong'})
        workspace = self.workspaces.create()
        self.workspace_id = workspace['id']
        job = self.workspaces.load_tables(
            workspace['id'],
            {table1_id: 'destination_1', table2_id: 'destination_2'}
        )
        self.jobs.block_until_completed(job['id'])

        job = self.tables.create_raw(
            bucket_id,
            'back-and-forth-table',
            data_workspace_id=workspace['id'],
            data_table_name='destination_1'
        )
        self.jobs.block_until_completed(job['id'])

        new_table = self.tables.detail(bucket_id + '.back-and-forth-table')
        self.assertEqual('back-and-forth-table', new_table['name'])

    # test load files into an abs workspace
    def test_load_files_to_workspace(self):
        if (os.getenv('SKIP_ABS_TEST')):
            self.skipTest('Skipping ABS test because env var SKIP_ABS_TESTS was set')
        # put a test file to storage
        file, path = tempfile.mkstemp(prefix='sapi-test')
        os.write(file, bytes('fooBar', 'utf-8'))
        os.close(file)

        # We'll put 2 files with the same tag to test multiple results
        file1_id = self.files.upload_file(path, tags=['sapi-client-python-tests', 'file1'])
        file2_id = self.files.upload_file(path, tags=['sapi-client-python-tests', 'file2'])

        file1 = self.files.detail(file1_id)
        file2 = self.files.detail(file2_id)
        # create a workspace and load the file to it
        workspace = self.workspaces.create('abs')
        self.workspace_id = workspace['id']
        job = self.workspaces.load_files(
            workspace['id'],
            {
                'tags': ['sapi-client-python-tests'],
                'destination': 'data/in/files'
            }
        )
        self.jobs.block_until_completed(job['id'])

        # assert that the file was loaded to the workspace
        blob_service_client = BlobServiceClient.from_connection_string(workspace['connection']['connectionString'])

        blob_client_1 = blob_service_client.get_blob_client(
            container=workspace['connection']['container'],
            blob='data/in/files/%s/%s/%s' % (file1['name'], str(file1['id']), str(file1['id']))
        )
        self.assertEqual('fooBar', blob_client_1.download_blob().readall().decode('utf-8'))

        blob_client_2 = blob_service_client.get_blob_client(
            container=workspace['connection']['container'],
            blob='data/in/files/%s/%s/%s' % (file2['name'], str(file2['id']), str(file2['id']))
        )
        self.assertEqual('fooBar', blob_client_2.download_blob().readall().decode('utf-8'))

    def __create_table(self, bucket_id, table_name, row):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',', quotechar='"')
            writer.writeheader()
            writer.writerow(row)
        return self.tables.create(name=table_name, file_path=path, bucket_id=bucket_id)
