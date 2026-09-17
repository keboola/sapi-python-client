import unittest

from kbcstorage.auth import BearerToken, StorageApiToken, coerce


class TestStorageApiToken(unittest.TestCase):
    def test_headers(self):
        self.assertEqual({'X-StorageApi-Token': 'dummy_token'},
                         StorageApiToken('dummy_token').headers())

    def test_empty_token(self):
        with self.assertRaisesRegex(ValueError, 'Token is required.'):
            StorageApiToken('')


class TestBearerToken(unittest.TestCase):
    def test_headers(self):
        self.assertEqual({'Authorization': 'Bearer kbc_at_dummy',
                          'X-KBC-ProjectId': '1234'},
                         BearerToken('kbc_at_dummy', '1234').headers())

    def test_numeric_project_id_is_stringified(self):
        token = BearerToken('kbc_at_dummy', 1234)
        self.assertEqual('1234', token.project_id)
        self.assertEqual('1234', token.headers()['X-KBC-ProjectId'])

    def test_empty_token(self):
        with self.assertRaisesRegex(ValueError, 'Token is required.'):
            BearerToken('', 1234)

    def test_missing_project_id(self):
        with self.assertRaisesRegex(ValueError, 'Project ID is required'):
            BearerToken('kbc_at_dummy', None)


class TestCoerce(unittest.TestCase):
    def test_bare_string_becomes_storage_api_token(self):
        auth = coerce('dummy_token')
        self.assertIsInstance(auth, StorageApiToken)
        self.assertEqual('dummy_token', auth.token)

    def test_strategy_passes_through(self):
        auth = BearerToken('kbc_at_dummy', 1234)
        self.assertIs(auth, coerce(auth))


if __name__ == '__main__':
    unittest.main()
