"""
Base classes for constructing the client.

Primarily exposes a base Endpoint class which deduplicates functionality across
various endpoints, such as tables, workspaces, jobs, etc. as described in the
`Storage API documentation`.


.. _Storage API documentation:
    http://docs.keboola.apiary.io/
"""
from kbcstorage.retry_requests import MAX_RETRIES_DEFAULT, RetryRequests
import requests


class Endpoint:
    """
    Base class for implementing a single endpoint related to a single entities
    as described in the Storage API.

    Attributes:
        base_url (str): The base URL for this endpoint.
        token (str): A key for the Storage API.
    """
    def __init__(self, root_url, path_component, token, max_requests_retries=MAX_RETRIES_DEFAULT):
        """
        Create an endpoint.

        Args
            root_url (str): Root url of API. eg.
                "https://connection.keboola.com/"
            path_component (str): The section of the path specific to the
                endpoint. eg. "buckets"
            token (str): A key for the Storage API. Can be found in the storage
                console.
        """
        if not root_url:
            raise ValueError("Root URL is required.")
        if not token:
            raise ValueError("Token is required.")
        self.root_url = root_url
        self.base_url = '{}/v2/storage/{}'.format(root_url.strip('/'),
                                                  path_component.strip('/'))
        self.token = token
        self._auth_header = {'X-StorageApi-Token': self.token,
                             'Accept-Encoding': 'gzip',
                             'User-Agent': 'Keboola Storage API Python Client'}
        self.requests = RetryRequests(max_requests_retries)

    def _get_raw(self, url, params=None, **kwargs):
        """
        Construct a requests GET call with args and kwargs and process the
        results.


        Args:
            url (str): requested url
            params (dict): additional url params to be passed to the underlying
                requests.get
            **kwargs: Key word arguments to pass to the get requests.get

        Returns:
            r (requests.Response): object

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = kwargs.pop('headers', {})
        headers.update(self._auth_header)

        r = self.requests.get(url, params=params, headers=headers, **kwargs)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise
        else:
            return r

    def _get(self, url, params=None, **kwargs):
        """
        Make authenticated GET request and return json

        Args:
            url (str): requested url
            params (dict): additional url params to be passed to the underlying
                requests.get
            **kwargs: Key word arguments to pass to the get requests.get

        Returns:
           body: Response body parsed from json.

        Raises:
            requests.HTTPError: If the API request fails.

        """
        return self._get_raw(url, params, **kwargs).json()

    def _post(self, *args, **kwargs):
        """
        Construct a requests POST call with args and kwargs and process the
        results.

        Args:
            *args: Positional arguments to pass to the post request.
            **kwargs: Key word arguments to pass to the post request.

        Returns:
            body: Response body parsed from json.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = kwargs.pop('headers', {})
        headers.update(self._auth_header)
        r = self.requests.post(headers=headers, *args, **kwargs)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise
        else:
            return r.json()

    def _put(self, *args, **kwargs):
        """
        Construct a requests PUT call with args and kwargs and process the
        results.

        Args:
            *args: Positional arguments to pass to the post request.
            **kwargs: Key word arguments to pass to the post request.

        Returns:
            body: Response body parsed from json.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = kwargs.pop('headers', {})
        headers.update(self._auth_header)
        r = self.requests.put(headers=headers, *args, **kwargs)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise
        else:
            return r.json()

    def _delete(self, *args, **kwargs):
        """
        Construct a requests DELETE call with args and kwargs and process the
        result

        Args:
            *args: Positional arguments to pass to the delete request.
            **kwargs: Key word arguments to pass to the delete request.

        Returns:
            body: Response body parsed from json.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = kwargs.pop('headers', {})
        headers.update(self._auth_header)
        r = self.requests.delete(headers=headers, *args, **kwargs)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise

        if 'application/json' in r.headers.get('Content-Type', ''):
            return r.json()
