"""
Test basic functionality of the Branches endpoint
"""
import unittest

import responses

from kbcstorage.branches import Branches
from tests.mocks.branches_responses import branches_metadata_response


class TestBranchesEndpointWithMocks(unittest.TestCase):
    """
    Test the methods of a Branches endpoint instance with mock HTTP responses
    """
    def setUp(self):
        token = 'dummy_token'
        base_url = 'https://connection.keboola.com/'
        self.branches = Branches(base_url, token)

    @responses.activate
    def test_metadata_no_branch(self):
        """
        Branches lists metadata correctly
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/branch/default/metadata',
                json=branches_metadata_response
            )
        )
        branch_metadata = self.branches.metadata()
        self.assertEqual(branches_metadata_response, branch_metadata)

    @responses.activate
    def test_metadata_some_branch(self):
        """
        Branches lists metadata correctly
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/branch/1234/metadata',
                json=branches_metadata_response
            )
        )
        branch_metadata = self.branches.metadata('1234')
        self.assertEqual(branches_metadata_response, branch_metadata)
