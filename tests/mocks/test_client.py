import unittest

from kbcstorage.client import BearerToken, Client

ENDPOINTS = ('buckets', 'files', 'jobs', 'tables', 'workspaces', 'components',
             'configurations', 'tokens', 'branches', 'triggers')


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

    def test_positional_args_still_accepted(self):
        client = Client('https://example.com', 'password', 'dev-123', False)
        self.assertEqual('dev-123', client.branch_id)
        self.assertFalse(hasattr(client, 'files'))

    def test_storage_api_token_reaches_every_endpoint(self):
        client = Client('https://example.com', 'password')
        self.assertIsNone(client.project_id)

        for name in ENDPOINTS:
            with self.subTest(endpoint=name):
                headers = getattr(client, name)._auth_header
                self.assertEqual('password', headers['X-StorageApi-Token'])
                self.assertNotIn('Authorization', headers)

    def test_bearer_token_reaches_every_endpoint(self):
        client = Client('https://example.com', BearerToken('kbc_at_dummy', 1234))
        self.assertEqual('kbc_at_dummy', client.token)
        self.assertEqual('1234', client.project_id)

        for name in ENDPOINTS:
            with self.subTest(endpoint=name):
                headers = getattr(client, name)._auth_header
                self.assertEqual('Bearer kbc_at_dummy', headers['Authorization'])
                self.assertEqual('1234', headers['X-KBC-ProjectId'])
                self.assertNotIn('X-StorageApi-Token', headers)

    def test_bearer_token_reaches_nested_metadata_endpoints(self):
        client = Client('https://example.com', BearerToken('kbc_at_dummy', 1234))

        for endpoint in (client.tables.metadata, client.configurations.metadata):
            with self.subTest(endpoint=endpoint):
                headers = endpoint._auth_header
                self.assertEqual('Bearer kbc_at_dummy', headers['Authorization'])
                self.assertNotIn('X-StorageApi-Token', headers)
