import csv
import os
import tempfile
from requests import exceptions
from kbcstorage.buckets import Buckets
from kbcstorage.jobs import Jobs
from kbcstorage.tables import Tables
from kbcstorage.workspaces import Workspaces
from tests.base_test_case import BaseTestCase


class TestEndpoint(BaseTestCase):
    def setUp(self):
        self.workspaces = Workspaces(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        self.buckets = Buckets(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        self.jobs = Jobs(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        self.tables = Tables(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        try:
            self.buckets.delete('in.c-py-test-buckets', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

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

    def __create_table(self, bucket_id, table_name, row):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',', quotechar='"')
            writer.writeheader()
            writer.writerow(row)
        return self.tables.create(name=table_name, file_path=path, bucket_id=bucket_id)
