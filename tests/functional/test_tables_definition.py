import os
import unittest
import warnings

from requests import exceptions

from kbcstorage.buckets import Buckets
from kbcstorage.dataclasses.tables import ColumnDefinition, Column
from kbcstorage.tables import Tables


class TestTables(unittest.TestCase):
    def setUp(self):
        self.tables = Tables(os.getenv('KBC_TEST_API_URL_SYNAPSE'),
                             os.getenv('KBC_TEST_TOKEN_SYNAPSE'))
        self.buckets = Buckets(os.getenv('KBC_TEST_API_URL_SYNAPSE'),
                               os.getenv('KBC_TEST_TOKEN_SYNAPSE'))
        try:
            self.buckets.delete('in.c-py-test-tables', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        self._test_bucket = self.buckets.create(name='py-test-tables', stage='in')
        # https://github.com/boto/boto3/issues/454
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            self.buckets.delete('in.c-py-test-tables', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_table_definition(self):
        columns = [Column('id', ColumnDefinition('INT')),
                   Column('name',
                          ColumnDefinition(type='NVARCHAR', length='200', nullable=False, default="'unnamed'"))]
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

        expected_definition = {'name': 'test_from_definition', 'primaryKeysNames': ['id'],
                               'columns': [{'name': 'id', 'definition': {'type': 'INT', 'nullable': True}},
                                           {'name': 'name',
                                            'definition': {'type': 'NVARCHAR', 'length': '200', 'nullable': False,
                                                           'default': "'unnamed'"}}],
                               'distribution': {'type': 'HASH', 'distributionColumnsNames': ['id']},
                               'index': {'type': 'CLUSTERED INDEX', 'indexColumnsNames': ['id']}, 'queue': 'main'}
        result_definition = self.tables.create_definition(self._test_bucket['id'],
                                                          name='test_from_definition',
                                                          primary_keys=['id'],
                                                          columns=columns,
                                                          distribution=distribution,
                                                          index=index)
        self.assertDictEqual(result_definition['operationParams'], expected_definition)
        self.assertEqual(result_definition['operationName'], 'tableDefinitionCreate')
