"""
Test basic functionality of the Jobs endpoint
"""
import unittest

import responses

from kbcstorage.jobs import Jobs

from .job_responses import list_response, detail_response


class TestJobsEndpointWithMocks(unittest.TestCase):
    """
    Test the methods of a Jobs endpoint instance with mock HTTP responses
    """
    def setUp(self):
        token = 'dummy_token'
        base_url = 'https://connection.keboola.com/v2/storage/'
        self.jobs = Jobs(base_url, token)

    @responses.activate
    def test_list(self):
        """
        Jobs mocks list correctly
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/jobs',
                json=list_response
            )
        )
        jobs_list = self.jobs.list()
        assert isinstance(jobs_list, list)

    @responses.activate
    def test_detail_by_id(self):
        """
        Jobs Endpoint can mock detail by integer id
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/jobs/22077337',
                json=detail_response
            )
        )
        jobs_id = 22077337
        jobs_detail = self.jobs.detail(jobs_id)
        assert jobs_detail['id'] == 22077337
