import os
import tempfile
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from kbcstorage.files import Files
from kbcstorage.workspaces import Workspaces
from requests import exceptions
from tests.base_test_case import BaseTestCase


class TestEndpoint(BaseTestCase):
    def setUp(self):
        self.workspaces = Workspaces(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        self.files = Files(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        try:
            file_list = self.files.list(tags=['sapi-client-python-tests'])
            for file in file_list:
                self.files.delete(file['id'])
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    # test load files into an abs workspace
    def test_load_files_to_workspace(self):
        if int(os.getenv('SKIP_ABS_TESTS', 0)):
            self.skipTest(f"Skipping ABS test because env var SKIP_ABS_TESTS was set: '{os.getenv('SKIP_ABS_TESTS')}'")
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
        self.workspaces.load_files(
            workspace['id'],
            {
                'tags': ['sapi-client-python-tests'],
                'destination': 'data/in/files'
            }
        )

        # assert that the file was loaded to the workspace
        blob_service_client = BlobServiceClient.from_connection_string(workspace['connection']['connectionString'])
        blob_client_1 = blob_service_client.get_blob_client(
            container=workspace['connection']['container'],
            blob='data/in/files/%s/%s' % (file1['name'], str(file1['id']))
        )
        self.assertEqual('fooBar', blob_client_1.download_blob().readall().decode('utf-8'))

        blob_client_2 = blob_service_client.get_blob_client(
            container=workspace['connection']['container'],
            blob='data/in/files/%s/%s' % (file2['name'], str(file2['id']))
        )
        self.assertEqual('fooBar', blob_client_2.download_blob().readall().decode('utf-8'))

        # now let's test that we can use the 'and' operator.  in this case file2 should not get loaded
        self.workspaces.load_files(
            workspace['id'],
            {
                'tags': ['sapi-client-python-tests', 'file1'],
                'operator': 'and',
                'destination': 'data/in/and_files'
            }
        )
        # file 1 should be there
        blob_client_1 = blob_service_client.get_blob_client(
            container=workspace['connection']['container'],
            blob='data/in/and_files/%s/%s' % (file1['name'], str(file1['id']))
        )
        self.assertEqual('fooBar', blob_client_1.download_blob().readall().decode('utf-8'))

        # file 2 should not
        blob_client_2 = blob_service_client.get_blob_client(
            container=workspace['connection']['container'],
            blob='data/in/and_files/%s/%s' % (file2['name'], str(file2['id']))
        )
        with self.assertRaises(ResourceNotFoundError) as context:
            blob_client_2.download_blob().readall().decode('utf-8')

        self.assertTrue('The specified blob does not exist' in str(context.exception))
