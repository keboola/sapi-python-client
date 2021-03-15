import csv
import os
import snowflake.connector
import tempfile
import time
import unittest
import warnings

from azure.storage.blob import BlobServiceClient
from requests import exceptions
from kbcstorage.workspaces import Workspaces
from kbcstorage.buckets import Buckets
from kbcstorage.tables import Tables
from kbcstorage.jobs import Jobs


class TestWorkspaces(unittest.TestCase):
    def setUp(self):
        self.workspaces = Workspaces(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        self.buckets = Buckets(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        self.tables = Tables(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        try:
            self.buckets.delete('in.c-py-test-buckets', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        # https://github.com/boto/boto3/issues/454
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            if self.workspace_id:
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
        bucket_id = self.buckets.create('py-test-tables')
        table1_id = self.__create_table(bucket_id, 'test-table-1', {'col1': 'ping', 'col2': 'pong'})
        table2_id = self.__create_table(bucket_id, 'test-table-2', {'col1': 'king', 'col2': 'kong'})
        workspace = self.workspaces.create()
        self.workspace_id = workspace['id']
        job = self.workspaces.load_tables(
            workspace['id'],
            {table1_id: 'destination_1', table2_id: 'destination_2'}
        )
        Jobs.block_until_completed(job['id'])
        conn = self.__get_snowflake_conenction(workspace['connection'])
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM "destination_1"')
        self.assertEquals(('ping', 'pong'), cursor.fetchone())
        cursor.execute('SELECT * FROM "destination_2"')
        self.assertEquals(('king', 'kong'), cursor.fetchone())

    # test load files into an abs workspace
    def test_load_files_from_workspace(self):
        workspace = self.workspaces.create('abs')
        self.workspace_id = workspace['id']
        job = self.workspaces.load_files(
            workspace['id'],
            {{'tags': ['sapi-client-pythen-tests']}}
        )
        Jobs.block_until_completed(job['id'])
        blob_service_client = BlobServiceClient.from_connection_string(workspace['connection']['connectionString'])
        container_client = blob_service_client.get_container_client(
            container=workspace['connection']['container']
        )
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            print(blob)

    def __create_table(self, bucket_id, table_name, row):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        with open(path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['col1', 'col2'],
                                    lineterminator='\n', delimiter=',', quotechar='"')
            writer.writeheader()
            writer.writerow(row)
        return self.tables.create(name=table_name, file_path=path, bucket_id=bucket_id)

    def __get_snowflake_conenction(self, connection_properties):
        return snowflake.connector.connect(
            user=connection_properties['user'],
            password=connection_properties['password'],
            account=connection_properties['host'].split('.snowflakecomputing')[0],
            warehouse=connection_properties['warehouse'],
            database=connection_properties['database'],
            schema=connection_properties['schema']
        )
