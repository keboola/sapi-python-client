import unittest

from kbcstorage.client import Client


class TestClient(unittest.TestCase):
    def test_token_not_updatable(self):
        "Token raises attribute error when updated"
        client = Client('https://example.com', 'password')
        with self.assertRaises(AttributeError):
            # noinspection PyPropertyAccess
            client.token = 'new-password'

    def test_url_trimmed(self):
        client = Client('https://example.com/', 'password')
        self.assertEqual(client.root_url, 'https://example.com')
