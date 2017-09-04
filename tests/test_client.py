import unittest

from kbcstorage.client import Client


class TestClient(unittest.TestCase):
    def test_token_update(self):
        "Token is updated for Endpoints as well as Client class"
        first_pass = "pass_1"
        second_pass = "pass_2"
        client = Client('https://example.com', first_pass)
        client.token = second_pass
        # Should be the same for all tokens
        assert client.token == client.tables.token
