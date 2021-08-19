"""
Manages calls to the Storage API relating to jobs.

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/jobs/
"""
import time

from kbcstorage.base import Endpoint


class Jobs(Endpoint):
    """
    Jobs are objects that manage asynchronous tasks, these are all
    potentially long-running actions such as loading table data,
    snapshotting, table structure modifications. Jobs are created by
    actions on target resources.

    A job has four available statuses:

    ``waiting``
        The job is in the queue and is waiting for execution.

    ``processing``
        The job is being processed by a worker.

    ``success``
        The job is done with a success.

    ``error``
        The job is done with an error.
    """

    def __init__(self, root_url, token):
        """
        Create a Jobs endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'jobs', token)

    def list(self):
        """
        List all jobs details.

        Returns:
            response_body: The json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        return self._get(self.base_url)

    def detail(self, job_id):
        """
        Retrieves information about a given job.

        Args:
            job_id (str or int): The id of the job.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}'.format(self.base_url, job_id)

        return self._get(url)

    def status(self, job_id):
        """
        Retrieve the status of a given job.

        Args:
            job_id (str or int): The id of the job.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        return self.detail(job_id)['status']

    def completed(self, job_id):
        """
        Check if a job is completed or not.

        Args:
            job_id (str or int): The id of the job.

        Returns:
            completed (bool): True if job is completed, else False.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        completed_statuses = ('error', 'success')
        return self.status(job_id) in completed_statuses

    def block_until_completed(self, job_id):
        """
        Poll the API until the job is completed.

        Args:
            job_id (str): The id of the job

        Returns:
            response_body: The parsed json from the HTTP response
                containing a storage Job.

        Raises:
            requests.HTTPError: If any API request fails.
        """
        retries = 1
        while True:
            job = self.detail(job_id)
            if job['status'] in ('error', 'success'):
                return job
            retries += 1
            time.sleep(min(2 ** retries, 20))

    def block_for_success(self, job_id):
        """
        Poll the API until the job is completed, then return ``True`` if the
        job is successful, else ``False``.

        Args:
            job_id (str): The id of the job

        Returns:
            success (bool): True if the job status is success, else False.

        Raises:
            requests.HTTPError: If any API request fails.
        """
        job = self.block_until_completed(job_id)
        return job['status'] == 'success'
