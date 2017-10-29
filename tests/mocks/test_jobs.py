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
        base_url = 'https://connection.keboola.com/'
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
        job_id = 22077337
        job_detail = self.jobs.detail(job_id)
        assert job_detail['id'] == 22077337

    @responses.activate
    def test_job_status(self):
        """
        Jobs mock status works correctly.
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/jobs/22077337',
                json=detail_response
            )
        )
        job_id = 22077337
        job_status = self.jobs.status(job_id)
        assert job_status == 'success'

    @responses.activate
    def test_job_completion(self):
        """
        Jobs mock completion check works correctly.
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/jobs/22077337',
                json=detail_response
            )
        )
        job_id = 22077337
        job_completed = self.jobs.completed(job_id)
        assert job_completed is True

    @responses.activate
    def test_job_blocking(self):
        """
        Jobs mock blocking polls until completion.
        """
        for _ in range(2):
            responses.add(
                responses.Response(
                    method='GET',
                    url=('https://connection.keboola.com/v2/storage/jobs/'
                         '22077337'),
                    json={'status': 'processing'}
                )
            )
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/jobs/22077337',
                json=detail_response
            )
        )
        job_id = '22077337'
        self.jobs.block_until_completed(job_id)
        assert True

    @responses.activate
    def test_success_blocking_if_success(self):
        """
        Jobs mock blocking polls until completion.
        """
        for _ in range(2):
            responses.add(
                responses.Response(
                    method='GET',
                    url=('https://connection.keboola.com/v2/storage/jobs/'
                         '22077337'),
                    json={'status': 'processing'}
                )
            )
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/jobs/22077337',
                json={'status': 'success'}
            )
        )
        job_id = '22077337'
        success = self.jobs.block_for_success(job_id)
        assert success is True

    @responses.activate
    def test_success_blocking_if_error(self):
        """
        Jobs mock blocking polls until completion.
        """
        for _ in range(2):
            responses.add(
                responses.Response(
                    method='GET',
                    url=('https://connection.keboola.com/v2/storage/jobs/'
                         '22077337'),
                    json={'status': 'processing'}
                )
            )
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/jobs/22077337',
                json={'status': 'error'}
            )
        )
        job_id = '22077337'
        success = self.jobs.block_for_success(job_id)
        assert success is False
