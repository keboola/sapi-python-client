"""
Manages calls to the Storage API relating to tokens

Full documentation https://keboola.docs.apiary.io/#reference/tokens-and-permissions/.

"""
import tempfile
import os
from kbcstorage.base import Endpoint
from kbcstorage.files import Files
from kbcstorage.jobs import Jobs


class Tokens(Endpoint):
    """
    Tables Endpoint
    """
    def __init__(self, root_url, token):
        """
        Create a Tables endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'tokens', token)

    def verify(self):
        """
        Verify token.

        Args:
            include (list): Properties to list (configuration, rows, state)
        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/verify'.format(self.base_url)
        return self._get(url)

