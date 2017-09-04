import unittest

from kbcstorage.client import Client


class TestClient(unittest.TestCase):
    def test_token_not_updatable(self):
        "Token raises attribute error when updated"
        client = Client('https://example.com', 'password')
        with self.assertRaises(AttributeError):
            client.token = 'new-password'
