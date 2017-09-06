"""
Test basic functionality of the Tables endpoint
"""
import unittest

import responses

from kbcstorage.tables import Tables

from .table_responses import list_response


class TestTablesEndpointWithMocks(unittest.TestCase):
    """
    Test the methods of a Tables endpoint instance with mock HTTP responses
    """
    def setUp(self):
        token = 'dummy_token'
        base_url = 'https://connection.keboola.com/'
        self.tables = Tables(base_url, token)

    @responses.activate
    def test_list(self):
        """
        Tables mocks list correctly
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/tables',
                json=list_response
            )
        )
        tables_list = self.tables.list()
        assert isinstance(tables_list, list)
