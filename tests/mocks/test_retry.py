"""
Test that requests are retried if the server is not available.
"""
import unittest
from unittest.mock import patch

import requests
import responses

from kbcstorage.base import Endpoint
from kbcstorage.tables import Tables

from .table_responses import list_response


class NoRetriesEndpoint(Endpoint):
    def __init__(self, root_url, token):
        super().__init__(root_url, 'no-retries', token, max_requests_retries=0)

    def list(self):
        return self._get(self.base_url)


class TestRequestRetry(unittest.TestCase):
    """
    Test that requests are retried.
    """
    def setUp(self):
        token = 'dummy_token'
        base_url = 'https://connection.keboola.com/'
        self.tables = Tables(base_url, token)
        self.no_retries = NoRetriesEndpoint(base_url, token)

    @responses.activate
    @patch('time.sleep', return_value=None)
    def test_ok(self, sleep_mock):
        """
        Retry will try at least 5 times.
        """
        for _ in range(4):
            responses.add(
                responses.Response(
                    method='GET',
                    url='https://connection.keboola.com/v2/storage/tables',
                    json=list_response,
                    status=502
                )
            )
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/tables',
                json=list_response,
            )
        )
        tables_list = self.tables.list()
        assert isinstance(tables_list, list)

    @responses.activate
    @patch('time.sleep', return_value=None)
    def test_raises_error_many_tries(self, sleep_mock):
        """
        Retry will fail if it gets enough of error responses.
        """
        for _ in range(20):
            responses.add(
                responses.Response(
                    method='GET',
                    url='https://connection.keboola.com/v2/storage/tables',
                    json=list_response,
                    status=502
                )
            )
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/tables',
                json=list_response,
            )
        )
        with self.assertRaises(requests.exceptions.HTTPError):
            self.tables.list()

    @responses.activate
    @patch('time.sleep', return_value=None)
    def test_raises_error_on_4xx(self, sleep_mock):
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/tables',
                json=list_response,
                status=401
            )
        )
        with self.assertRaises(requests.exceptions.HTTPError):
            self.tables.list()

    @responses.activate
    @patch('time.sleep', return_value=None)
    def test_no_retries(self, sleep_mock):
        """
        Request wont be retried for endpoints that configured it.
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/no-retries',
                json=list_response,
                status=502
            )
        )
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/no-retries',
                json=list_response,
            )
        )
        with self.assertRaises(requests.exceptions.HTTPError):
            self.no_retries.list()
