"""
Base classes for constructing the client.

Primarily exposes a base Endpoint class which deduplicates functionality across
various endpoints, such as tables, workspaces, jobs, etc. as described in the
`Storage API documentation`.


.. _Storage API documentation:
    http://docs.keboola.apiary.io/
"""
from urllib.parse import urljoin

import requests


class Endpoint:
    """
    Base class for implementing a single endpoint related to a single entities
    as described in the Storage API.

    Attributes:
        path (str): URL for this endpoint.
        token (str): A key for the Storage API.
    """
    def __init__(self, root, extension, token):
        """
        Create an endpoint.

        Args
            root (str): Root url of API. eg.
                "https://connection.keboola.com/v2/storage/"
            extension (str): Extension of url for the endpoint. eg. "buckets"
            token (str): A key for the Storage API. Can be found in the storage
            console.
        """
        self.path = urljoin(root, extension)
        self.token = token

    def get(self, *args, **kwargs):
        """
        Construct a requests GET call with args and kwargs and process the
        results.

        Args:
            *args: Positional arguments to pass to the get request.
            **kwargs: Key word arguments to pass to the get request.

        Returns:
            body: Response body parsed from json.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        r = requests.get(*args, **kwargs)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise
        finally:
            return r.json()

    def post(self, *args, **kwargs):
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
        r = requests.post(*args, **kwargs)
        r.raise_for_status()
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise
        finally:
            return r.json()

    def _put(self):
        raise NotImplementedError

    def delete(self, *args, **kwargs):
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
        r = requests.delete(*args, **kwargs)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise

    def _extend(self, base, parts):
        """
        Join the items in parts to base by forward slashes

        Args:
            base (:obj:`str`): The base string, eg.
                'http://example.com/hello/'.
            parts (:obj:`list`): The extensions, eg ['a', 'deeper', 'path'].

        Returns:
            joined (:obj:`str`): The parts joined to base by forward slash,
                eg. 'http://example.com/hello/a/deeper/path'.
        """
        parts.insert(0, base)
        return '/'.join([str(part).strip('/') for part in parts if
                         str(part).strip('/')])
