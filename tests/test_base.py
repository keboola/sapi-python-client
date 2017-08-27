import unittest

from requests import HTTPError

from kbcstorage.base import Endpoint


class TestEndpoint(unittest.TestCase):
    """
    Test Endpoint functionality.
    """
    def setUp(self):
        self.root = 'https://httpbin.org'
        self.token = ''

    def test_get(self):
        """
        Simple get works.
        """
        endpoint = Endpoint(self.root, 'get', self.token)
        requested_url = endpoint.get(endpoint.base_url)['url']
        assert requested_url == 'https://httpbin.org/get'

    def test_get_404(self):
        """
        Get inexistent resource raises HTTPError.
        """
        endpoint = Endpoint(self.root, 'get', self.token)
        with self.assertRaises(HTTPError):
            endpoint.get('{}/not-a-url'.format(endpoint.base_url))

    def test_post(self):
        """
        Simple post works.
        """
        endpoint = Endpoint(self.root, 'post', self.token)
        requested_url = endpoint.post(endpoint.base_url)['url']
        assert requested_url == 'https://httpbin.org/post'

    def test_post_404(self):
        """
        Post to inexistent resource raises HTTPError.
        """
        endpoint = Endpoint(self.root, 'post', self.token)
        with self.assertRaises(HTTPError):
            endpoint.post('{}/not-a-url'.format(endpoint.base_url))

    def test_delete(self):
        """
        Simple delete works.
        """
        endpoint = Endpoint(self.root, 'delete', self.token)
        resp = endpoint.delete(endpoint.base_url)
        assert resp is None

    def test_delete_404(self):
        """
        Delete inexistent resource raises HTTPError.
        """
        endpoint = Endpoint(self.root, 'delete', self.token)
        with self.assertRaises(HTTPError):
            endpoint.delete('{}/not-a-url'.format(endpoint.base_url))
