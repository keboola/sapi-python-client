import unittest
import os
from requests import HTTPError
from kbcstorage.base import Endpoint


class TestEndpoint(unittest.TestCase):
    """
    Test Endpoint functionality.
    """
    def setUp(self):
        self.root = os.getenv('KBC_TEST_API_URL')
        self.token = 'some-token'

    def test_get(self):
        endpoint = Endpoint(self.root, '', self.token)
        self.assertEqual(os.getenv('KBC_TEST_API_URL'), endpoint.root_url)
        self.assertEqual(os.getenv('KBC_TEST_API_URL') + '/v2/storage/',
                         endpoint.base_url)
        self.assertEqual('some-token',
                         endpoint.token)

    def test_get_404(self):
        endpoint = Endpoint(self.root, 'not-a-url', self.token)
        self.assertEqual(os.getenv('KBC_TEST_API_URL') +
                         '/v2/storage/not-a-url',
                         endpoint.base_url)
        with self.assertRaises(HTTPError):
            endpoint._get(endpoint.base_url)

    def test_get_404_2(self):
        endpoint = Endpoint(self.root, '', self.token)
        self.assertEqual(os.getenv('KBC_TEST_API_URL') +
                         '/v2/storage/',
                         endpoint.base_url)
        with self.assertRaises(HTTPError):
            endpoint._get('{}/not-a-url'.format(endpoint.base_url))

    def test_post_404(self):
        """
        Post to inexistent resource raises HTTPError.
        """
        endpoint = Endpoint(self.root, '', self.token)
        with self.assertRaises(HTTPError):
            endpoint._post('{}/not-a-url'.format(endpoint.base_url))

    def test_delete_404(self):
        """
        Delete inexistent resource raises HTTPError.
        """
        endpoint = Endpoint(self.root, 'delete', self.token)
        with self.assertRaises(HTTPError):
            endpoint._delete('{}/not-a-url'.format(endpoint.base_url))

    def test_custom_headers(self):
        """
        Passing custom headers to Endpoint._get()
        """
        endpoint = Endpoint(self.root, '', self.token)
        resp = endpoint._get(self.root, headers={'x-foo':'bar'})
        request_headers = resp.request.headers
        self.assertIn('x-foo', request_headers)
        self.assertIn('X-StorageApi-Token', request_headers)
        self.assertEqual('bar', request_headers['x-foo'])
