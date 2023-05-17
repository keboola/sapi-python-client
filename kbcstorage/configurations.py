"""
Manages calls to the Storage API relating to configurations

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/tables/
"""
import tempfile
import os
from kbcstorage.base import Endpoint
from kbcstorage.files import Files
from kbcstorage.jobs import Jobs


class Configurations(Endpoint):
    """
    Configurations Endpoint
    """
    def __init__(self, root_url, token):
        """
        Create a Configuration endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'branch/default/components', token)

    def list(self, include=None):
        """
        List all components (and optionally configurations) in a project.

        Args:
            include (list): Properties to list (configuration, rows, state)
        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        params = {'include': ','.join(include)} if include else {}
        return self._get(self.base_url, params=params)
