"""
Test basic functionality of the Tables endpoint
"""
import unittest

import responses

from kbcstorage.dataclasses.tables import Column, ColumnDefinition
from kbcstorage.tables import Tables
from .bucket_responses import create_definition_response
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
    def test_create_definition(self):
        columns = [Column('id', ColumnDefinition('INT')),
                   Column('name',
                          ColumnDefinition(type='NVARCHAR', length='200', nullable=True, default="'unnamed'"))]
        distribution = {
            "type": "HASH",
            "distributionColumnsNames": [
                "id"
            ]
        }

        index = {
            "type": "CLUSTERED INDEX",
            "indexColumnsNames": [
                "id"
            ]
        }

        expected_request = {'name': 'test_from_definition', 'primaryKeysNames': ['id'],
                            'columns': [{'name': 'id',
                                         'definition': {'type': 'INT'}},
                                        {'name': 'name',
                                         'definition': {'type': 'NVARCHAR', 'length': '200', 'nullable': True,
                                                        'default': "'unnamed'"}}],
                            'distribution': {'type': 'HASH', 'distributionColumnsNames': ['id']},
                            'index': {'type': 'CLUSTERED INDEX', 'indexColumnsNames': ['id']}}
        request_matcher = responses.matchers.json_params_matcher(expected_request)

        responses.add(
            responses.Response(
                method='POST',
                url='https://connection.keboola.com/v2/storage/buckets/in.c-test/tables-definition',
                json=create_definition_response,
                match=[request_matcher]
            )
        )

        responses.add(responses.Response(method='GET',
                                         url='https://connection.keboola.com/v2/storage/jobs/145390544',
                                         json={"status": "success"}))

        result_definition = self.tables.create_definition('in.c-test',
                                                          name='test_from_definition',
                                                          primary_keys=['id'],
                                                          columns=columns,
                                                          distribution=distribution,
                                                          index=index)

        self.assertDictEqual(result_definition, result_definition)
