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

    def _get(self, params=[], extra_headers={}):
        """
        Make a get request to the url of the endpoint extended with additional
        params.

        Args:
            params (:obj:`list`): Is used to update the url of the request.
                Default [].
            extra_headers (:obj:`dict`): Is used to update the headers.
                Default {}.

        Returns:
            body: Response body parsed from json.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = {'X-StorageApi-Token': self.token}
        headers.update(extra_headers)

        url = self._extend(self.path, params)

        r = requests.get(url, headers=headers)
        r.raise_for_status()

        return r.json()

    def _post(self, body={}, params=[], extra_headers={}):
        """
        Make a post request to the endpoint url extended with params,

        Args:
            body (:obj:`dict`): key value pairs for the body of the HTTP
                request. Default {}.
            params (:obj:`list`): Is used to update the url of the request.
                Default [].
            extra_headers (:obj:`dict`): Is used to update the headers.
                Default {}.

        Returns:
            body: Response body parsed from json.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = {
            'X-StorageApi-Token': self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        headers.update(headers)

        url = self._extend(self.path, params)

        r = requests.post(url, headers=headers, data=body)
        r.raise_for_status()
        return r.json()

    def _put(self):
        raise NotImplementedError

    def _delete(self, params=[], extra_headers={}):
        """
        Make a delete request to the endpoint.

        Args:
            params (:obj:`list`): Is used to update the url of the request.
                Default [].
            extra_headers (:obj:`dict`): Is used to update the headers.
                Default {}.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = {
            'X-StorageApi-Token': self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        headers.update(extra_headers)

        url = self._extend(self.path, params)

        r = requests.delete(url, headers=headers)
        r.raise_for_status()

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
        return '/'.join([base, *[str(part).strip('/') for part in parts if
                         str(part).strip('/')]])
