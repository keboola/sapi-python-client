import unittest
import responses
from kbcstorage.components import Components
from .component_responses import list_response, branch_list_response


class TestComponentsWithMocks(unittest.TestCase):
    def setUp(self):
        self.token = 'dummy_token'
        self.base_url = 'https://connection.keboola.com/'

    @responses.activate
    def test_list_main_branch(self):
        """
        Components mocks list correctly.
        """
        self.components = Components(self.base_url, self.token, 'default')

        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/branch/default/components',
                json=list_response
            )
        )
        components_list = self.components.list()
        self.assertIsInstance(components_list, list)

    @responses.activate
    def test_list_other_branch(self):
        """
        Components mocks list correctly.
        """
        self.components = Components(self.base_url, self.token, '1234')

        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/branch/1234/components',
                json=branch_list_response
            )
        )
        components_list = self.components.list()
        self.assertTrue(isinstance(components_list, list))
