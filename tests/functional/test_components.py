import os
from requests import exceptions
from kbcstorage.components import Components
from kbcstorage.configurations import Configurations
from tests.base_test_case import BaseTestCase


class TestEndpoint(BaseTestCase):
    def setUp(self):
        self.components = Components(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'), 'default')
        self.configurations = Configurations(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'), 'default')
        self.configurations.create(self.TEST_COMPONENT_NAME, 'test_components')

    def tearDown(self):
        try:
            for configuration in self.configurations.list(self.TEST_COMPONENT_NAME):
                self.configurations.delete(self.TEST_COMPONENT_NAME, configuration['id'])
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def testListComponents(self):
        components = self.components.list()
        self.assertTrue(len(components) > 0)
        for component in components:
            with self.subTest():
                self.assertTrue('id' in component)
                self.assertTrue('name' in component)
                self.assertTrue('type' in component)
                self.assertTrue('uri' in component)

            with self.subTest():
                for configuration in component['configurations']:
                    self.assertTrue('id' in configuration)
                    self.assertTrue('name' in configuration)
                    self.assertTrue('description' in configuration)
                    self.assertFalse('configuration' in configuration)
                    self.assertFalse('rows' in configuration)
                    self.assertFalse('state' in configuration)

    def testListComponentsIncludeConfigurations(self):
        components = self.components.list(include=['configuration', 'rows', 'state'])
        self.assertTrue(len(components) > 0)
        for component in components:
            with self.subTest():
                self.assertTrue('id' in component)
                self.assertTrue('name' in component)
                self.assertTrue('type' in component)
                self.assertTrue('uri' in component)

            with self.subTest():
                self.assertTrue('configurations' in component)
                for configuration in component['configurations']:
                    self.assertTrue('id' in configuration)
                    self.assertTrue('name' in configuration)
                    self.assertTrue('description' in configuration)
                    self.assertTrue('configuration' in configuration)
                    self.assertTrue('rows' in configuration)
                    self.assertTrue('state' in configuration)
