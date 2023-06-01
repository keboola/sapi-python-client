""""
Entry point for the Storage API client.
"""
from kbcstorage.branches import Branches
from kbcstorage.buckets import Buckets
from kbcstorage.components import Components
from kbcstorage.configurations import Configurations
from kbcstorage.tokens import Tokens
from kbcstorage.workspaces import Workspaces
from kbcstorage.jobs import Jobs
from kbcstorage.tables import Tables
from kbcstorage.files import Files


class Client:
    """
    Storage API Client.
    """

    def __init__(self, api_domain, token, branch_id='default'):
        """
        Initialise a client.

        Args:
            api_domain (str): The domain on which the API sits. eg.
                "https://connection.keboola.com".
            token (str): A storage API key.
            branch_id (str): The ID of branch to use, use 'default' to work without branch (in main).
        """
        self.root_url = api_domain.rstrip("/")
        self._token = token
        self._branch_id = branch_id

        self.buckets = Buckets(self.root_url, self.token)
        self.files = Files(self.root_url, self.token)
        self.jobs = Jobs(self.root_url, self.token)
        self.tables = Tables(self.root_url, self.token)
        self.workspaces = Workspaces(self.root_url, self.token)
        self.components = Components(self.root_url, self.token, self.branch_id)
        self.configurations = Configurations(self.root_url, self.token, self.branch_id)
        self.tokens = Tokens(self.root_url, self.token)
        self.branches = Branches(self.root_url, self.token)

    @property
    def token(self):
        return self._token

    @property
    def branch_id(self):
        return self._branch_id
