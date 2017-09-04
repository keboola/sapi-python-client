""""
Entry point for the Storage API client.
"""

from kbcstorage.buckets import Buckets
from kbcstorage.workspaces import Workspaces
from kbcstorage.jobs import Jobs
from kbcstorage.tables import Tables
from kbcstorage.files import Files


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
        self.root_url = api_domain
        self.token = token

        self.buckets = Buckets(self.root_url, self.token)
        self.workspaces = Workspaces(self.root_url, self.token)
        self.jobs = Jobs(self.root_url, self.token)
        self.tables = Tables(self.root_url, self.token)
        self.files = Files(self.root_url, self.token)
