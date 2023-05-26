import unittest
import responses
from kbcstorage.configurations import Configurations
from .configuration_responses import list_response, detail_response, create_response


class TestComponentsWithMocks(unittest.TestCase):
    def setUp(self):
        self.token = 'dummy_token'
        self.base_url = 'https://connection.keboola.com/'
        self.configurations = Configurations(self.base_url, self.token, 'default')

    @responses.activate
    def test_list(self):
        """
        Configuration mocks list correctly.
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/branch/default/components/some-component/configs',
                json=list_response
            )
        )
        configurations_list = self.configurations.list('some-component')
        self.assertIsInstance(configurations_list, list)

    @responses.activate
    def test_detail_by_id(self):
        """
        Configuration mocks detail by id correctly.
        """
        responses.add(
            responses.Response(
                method='GET',
                url=(
                    'https://connection.keboola.com/v2/storage/branch/default/components/keboola.runner-config-test'
                    '/configs/978755777'),
                json=detail_response
            )
        )
        configuration_id = '978755777'
        bucket_detail = self.configurations.detail('keboola.runner-config-test', configuration_id)
        self.assertEqual(bucket_detail['id'], '978755777')

    @responses.activate
    def test_delete(self):
        """
        Configuration mock deletes configuration by id.
        """
        responses.add(
            responses.Response(
                method='DELETE',
                url='https://connection.keboola.com/v2/storage/branch/default/components/keboola.runner-config-test'
                    '/configs/some-id',
                json={}
            )
        )
        configuration_id = 'some-id'
        deleted_detail = self.configurations.delete('keboola.runner-config-test', configuration_id)
        self.assertIsNone(deleted_detail)

    @responses.activate
    def test_create(self):
        """
        Configuration mock creates new configuration.
        """
        responses.add(
            responses.Response(
                method='POST',
                url='https://connection.keboola.com/v2/storage/branch/default/components/keboola.runner-config-test'
                    '/configs',
                json=create_response
            )
        )
        created_detail = self.configurations.create(
            component_id='keboola.runner-config-test',
            name='some-configuration',
            description='my-description',
            configuration={'parameters': {'foo': 'bar'}},
            is_disabled=False,
            change_description='some-change-description',
            configuration_id='some-configuration-id',
        )
        self.assertTrue(created_detail['id'], 'some-configuration_id')
