"""
Manages calls to the Storage API relating to buckets

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/buckets/
"""
from kbcstorage.base import Endpoint
from kbcstorage.jobs import Jobs


class Buckets(Endpoint):
    """
    Buckets Endpoint
    """
    def __init__(self, root_url, token):
        """
        Create a Buckets endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'buckets', token)

    def list(self):
        """
        List all buckets in project.

        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """

        return self._get(self.base_url)

    def list_tables(self, bucket_id, include=None):
        """
        List all tables in a bucket.

        Args:
            bucket_id (str): Id of the bucket
            include (list): Properties to list (attributes, columns)
        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = {'X-StorageApi-Token': self.token}

        url = '{}/{}/tables'.format(self.base_url, bucket_id)
        params = {}
        if include is not None and isinstance(include, list):
            params['include'] = ','.join(include)
        return self._get(url, headers=headers, params=params)

    def detail(self, bucket_id):
        """
        Retrieves information about a given bucket.

        Args:
            bucket_id (str): The id of the bucket.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}'.format(self.base_url, bucket_id)

        return self._get(url)

    def create(self, name, stage='in', description='', backend=None):
        """
        Create a new bucket.

        Args:
            name (str): The new bucket name (only alphanumeric and underscores)
            stage (str): The new bucket stage. Can be one of ``in`` or ``out``.
                Default ``in``.
            description (str): The new bucket description.
            backend (str): The new bucket backend. Cand be one of
                ``snowflake``, ``redshift`` or ``mysql``. Default determined by
                project settings.
        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        # Separating create and link into two distinct functions...
        # Need to check args...
        body = {
            'name': name,
            'stage': stage,
            'description': description,
            'backend': backend
        }

        return self._post(self.base_url, data=body)

    def delete(self, bucket_id, force=False, asynchronous=True):
        """
        Delete a bucket referenced by ``bucket_id``.

        By default, only empty buckets without dependencies (aliases etc) can
        be deleted. The optional ``force`` parameter allows for the deletion
        of non-empty buckets.

        Args:
            bucket_id (str): The id of the bucket to be deleted.
            force (bool): If ``True``, deletes the bucket even if it is not
                empty. Default ``False``.
            asynchronous (bool): if asynchronous then the
        """
        # How does the API handle it when force == False and the bucket is non-
        # empty?
        url = '{}/{}'.format(self.base_url, bucket_id)
        params = {'force': force, 'async': asynchronous}
        if (asynchronous):
            job = self._delete(url, params=params)
            jobs = Jobs(self.root_url, self.token)
            job = jobs.block_until_completed(job['id'])
            if job['status'] == 'error':
                raise RuntimeError(job['error']['message'])
        else:
            self._delete(url, params=params)

    def link(self, *args, **kwargs):
        """
        **Not implemented**

        Link an existing bucket from another project.

        Creates a new bucket which contains the contents of a shared bucket in
        a source project. Linking a bucket from another project is only
        possible if it has been enabled in the project.
        """
        raise NotImplementedError

    def share(self, *args, **kwargs):
        """
        **Not implemented**

        Enable sharing of a bucket.

        The bucket will be shared to the entire organisation to which the
        project belongs. It may then be shared to any project of that
        organization. This operation is only available to administrator tokens.
        """
        raise NotImplementedError

    def unshare(self, *args, **kwargs):
        """
        **Not implemented**

        Stop sharing a bucket.

        The bucket must not be linked to other projects. To unshare an already
        linked bucket, the links must first be deleted - use ``delete`` on the
        bucket in the linking project. This operation is only available for
        administrator tokens.
        """
        raise NotImplementedError
