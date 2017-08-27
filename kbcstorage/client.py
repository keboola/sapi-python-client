"""
Entry point for the Storage API client.
"""

from kbcstorage.buckets import Buckets
from kbcstorage.workspaces import Workspaces
from kbcstorage.jobs import Jobs


class Client:
    """
    Storage API Client.
    """
    def __init__(self, api_domain, token):
        """
        Initialise a client.

        Args:
            api_domain (str): The domain on which the API sits. eg.
                "https://connection.keboola.com".
            token (str): A storage API key.
        """
        api_version_string = 'v2'
        api_path_component = 'storage'

        self.root_url = '{}/{}/{}'.format(api_domain,
                                          api_version_string,
                                          api_path_component)
        self.token = token

        self.buckets = Buckets(self.root_url, self.token)
        self.workspaces = Workspaces(self.root_url, self.token)
        self.jobs = Jobs(self.root_url, self.token)
