import os

from kbcstorage.branches import Branches
from tests.base_test_case import BaseTestCase


class TestEndpoint(BaseTestCase):
    def setUp(self):
        self.branches = Branches(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))

    def test_metadata(self):
        metadata = self.branches.metadata('default')
        self.assertTrue(isinstance(metadata, list))
