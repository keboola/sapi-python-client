"""
Test basic functionality of the Tables endpoint
"""
import unittest
from urllib.parse import parse_qs

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

    @responses.activate
    def test_export_raw_sends_limit_and_format(self):
        """
        export_raw passes limit and format to the export-async request
        """
        responses.add(
            responses.Response(
                method='POST',
                url='https://connection.keboola.com/v2/storage/tables/in.c-bucket.table/export-async',
                json={'id': 1, 'status': 'waiting'}
            )
        )
        self.tables.export_raw('in.c-bucket.table', limit=10, file_format='escaped')
        request_body = parse_qs(responses.calls[0].request.body, keep_blank_values=True)
        assert request_body['limit'] == ['10']
        assert request_body['format'] == ['escaped']

    @responses.activate
    def test_export_raw_omits_limit_when_not_set(self):
        """
        export_raw does not send limit when it is None
        """
        responses.add(
            responses.Response(
                method='POST',
                url='https://connection.keboola.com/v2/storage/tables/in.c-bucket.table/export-async',
                json={'id': 1, 'status': 'waiting'}
            )
        )
        self.tables.export_raw('in.c-bucket.table')
        request_body = parse_qs(responses.calls[0].request.body, keep_blank_values=True)
        assert 'limit' not in request_body
        assert request_body['format'] == ['rfc']

    def test_export_raw_rejects_invalid_limit(self):
        """
        export_raw raises ValueError for a limit that is not a positive int
        """
        for invalid_limit in ('10', 10.5, True, 0, -1):
            with self.subTest(limit=invalid_limit):
                with self.assertRaises(ValueError):
                    self.tables.export_raw('in.c-bucket.table', limit=invalid_limit)
