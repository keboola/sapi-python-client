"""
Test basic functionality of the Tokens endpoint
"""
import unittest

import responses

from kbcstorage.tokens import Tokens
from tests.mocks.token_responses import verify_token_response


class TestTokensEndpointWithMocks(unittest.TestCase):
    """
    Test the methods of a Tokens endpoint instance with mock HTTP responses
    """
    def setUp(self):
        token = 'dummy_token'
        base_url = 'https://connection.keboola.com/'
        self.tokens = Tokens(base_url, token)

    @responses.activate
    def test_verify(self):
        """
        Verify token returns correctly
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/tokens/verify',
                json=verify_token_response
            )
        )
        token_info = self.tokens.verify()
        self.assertEqual(verify_token_response, token_info)
