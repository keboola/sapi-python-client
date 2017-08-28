"""
Base classes for constructing the client.

Primarily exposes a base Endpoint class which deduplicates functionality across
various endpoints, such as tables, workspaces, jobs, etc. as described in the
`Storage API documentation`.


.. _Storage API documentation:
    http://docs.keboola.apiary.io/
"""
import requests


class Endpoint:
    """
    Base class for implementing a single endpoint related to a single entities
    as described in the Storage API.

    Attributes:
        base_url (str): The base URL for this endpoint.
        token (str): A key for the Storage API.
    """
    def __init__(self, root_url, path_component, token):
        """
        Create an endpoint.

        Args
            root_url (str): Root url of API. eg.
                "https://connection.keboola.com/v2/storage/"
            path_component (str): The section of the path specific to the
                endpoint. eg. "buckets"
            token (str): A key for the Storage API. Can be found in the storage
                console.
        """
        self.root_url = root_url
        self.base_url = '{}/v2/storage/{}'.format(root_url.strip('/'), path_component.strip('/'))
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
        else:
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
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise
        else:
            return r.json()

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
        # Should delete return something on success?
