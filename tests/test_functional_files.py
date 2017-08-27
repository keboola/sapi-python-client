import os
import unittest
import tempfile
from requests import exceptions
from kbcstorage.files import Files
import warnings


class TestFunctionalBuckets(unittest.TestCase):
    def setUp(self):
        self.files = Files(os.getenv('KBC_TEST_API_URL') + '/v2/storage/',
                           os.getenv('KBC_TEST_TOKEN'))
        files = self.files.list(tags=['py-test'])
        for file in files:
            self.files.delete(file['id'])
        # https://github.com/boto/boto3/issues/454
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        files = self.files.list(tags=['py-test'])
        for file in files:
            self.files.delete(file['id'])

    def test_create_file(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        os.write(file, bytes('fooBar', 'utf-8'))
        file_id = self.files.upload_file(path, tags=['py-test', 'file1'])
        os.close(file)
        file_info = self.files.detail(file_id)
        self.assertEqual(file_id, file_info['id'])
        self.assertEqual(6, file_info['sizeBytes'])
        self.assertFalse(file_info['isPublic'])
        self.assertFalse(file_info['isSliced'])
        self.assertTrue(file_info['isEncrypted'])
        self.assertTrue('name' in file_info)
        self.assertTrue('created' in file_info)
        self.assertTrue('url' in file_info)
        self.assertTrue('region' in file_info)
        self.assertTrue('creatorToken' in file_info)
        self.assertTrue('tags' in file_info)
        self.assertTrue('py-test' in file_info['tags'])
        self.assertTrue('file1' in file_info['tags'])

    def test_delete_file(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        os.write(file, bytes('fooBar', 'utf-8'))
        file_id = self.files.upload_file(path, tags=['py-test', 'file1'])
        os.close(file)
        self.assertEqual(file_id, self.files.detail(file_id)['id'])
        self.files.delete(file_id)
        try:
            self.assertEqual(file_id, self.files.detail(file_id)['id'])
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_download_file(self):
        file, path = tempfile.mkstemp(prefix='sapi-test')
        os.write(file, bytes('fooBar', 'utf-8'))
        file_id = self.files.upload_file(path, tags=['py-test', 'file1'])
        os.close(file)
        self.assertEqual(file_id, self.files.detail(file_id, federation_token=True)['id'])
        self.assertEqual(['py-test', 'file1'], self.files.detail(file_id)['tags'])
