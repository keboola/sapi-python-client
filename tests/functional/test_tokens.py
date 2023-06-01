import os
from kbcstorage.tokens import Tokens
from tests.base_test_case import BaseTestCase


class TestEndpoint(BaseTestCase):
    def setUp(self):
        self.tokens = Tokens(os.getenv('KBC_TEST_API_URL'),
                             os.getenv('KBC_TEST_TOKEN'))

    def test_verify(self):
        token_info = self.tokens.verify()
        self.assertTrue('id' in token_info)
        self.assertTrue('description' in token_info)
        self.assertTrue('canManageBuckets' in token_info)
        self.assertTrue('owner' in token_info)
