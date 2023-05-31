import os
from requests import HTTPError
from kbcstorage.base import Endpoint
from tests.base_test_case import BaseTestCase


class TestEndpoint(BaseTestCase):
    """
    Test Endpoint functionality.
    """
    def setUp(self):
        self.root = os.getenv('KBC_TEST_API_URL')
        self.token = 'some-token'

    def test_props(self):
        endpoint = Endpoint(self.root, '/', self.token)
        with self.subTest():
            self.assertEqual(os.getenv('KBC_TEST_API_URL'), endpoint.root_url)
        with self.subTest():
            self.assertEqual(os.getenv('KBC_TEST_API_URL') + '/v2/storage/',
                             endpoint.base_url)
        with self.subTest():
            self.assertEqual('some-token',
                             endpoint.token)

    def test_get(self):
        endpoint = Endpoint(self.root, '/', self.token)
        with self.subTest():
            self.assertEqual(os.getenv('KBC_TEST_API_URL'), endpoint.root_url)
        with self.subTest():
            self.assertEqual(os.getenv('KBC_TEST_API_URL') + '/v2/storage/',
                             endpoint.base_url)
        with self.subTest():
            self.assertEqual('some-token',
                             endpoint.token)
        response = endpoint._get(endpoint.base_url)
        with self.subTest():
            self.assertEqual('storage', response['api'])
        with self.subTest():
            self.assertEqual('v2', response['version'])
        with self.subTest():
            self.assertIn('components', response)
        with self.subTest():
            self.assertIn('services', response)

    def test_get_404(self):
        endpoint = Endpoint(self.root, 'not-a-url', self.token)
        with self.subTest():
            self.assertEqual(os.getenv('KBC_TEST_API_URL') +
                             '/v2/storage/not-a-url',
                             endpoint.base_url)
        with self.subTest():
            with self.assertRaises(HTTPError):
                endpoint._get(endpoint.base_url)

    def test_get_404_2(self):
        endpoint = Endpoint(self.root, '/', self.token)
        with self.subTest():
            self.assertEqual(os.getenv('KBC_TEST_API_URL') +
                             '/v2/storage/',
                             endpoint.base_url)
        with self.subTest():
            with self.assertRaises(HTTPError):
                endpoint._get('{}/not-a-url'.format(endpoint.base_url))

    def test_post_404(self):
        """
        Post to inexistent resource raises HTTPError.
        """
        endpoint = Endpoint(self.root, '/', self.token)
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
        endpoint = Endpoint(self.root, '/', self.token)
        resp = endpoint._get_raw(self.root + '/v2/storage', headers={'x-foo': 'bar'})
        request_headers = resp.request.headers
        with self.subTest():
            self.assertIn('x-foo', request_headers)
        with self.subTest():
            self.assertIn('X-StorageApi-Token', request_headers)
        with self.subTest():
            self.assertEqual('bar', request_headers['x-foo'])

    def test_missing_url(self):
        with self.assertRaisesRegex(ValueError, "Root URL is required."):
            Endpoint(None, '', None)

    def test_missing_token(self):
        with self.assertRaisesRegex(ValueError, "Token is required."):
            Endpoint('https://connection.keboola.com/', 'tables', None)
