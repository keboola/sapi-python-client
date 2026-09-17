""""
Entry point for the Storage API client.
"""
from kbcstorage.auth import BearerToken, StorageApiToken, coerce  # noqa: F401
from kbcstorage.branches import Branches
from kbcstorage.buckets import Buckets
from kbcstorage.components import Components
from kbcstorage.configurations import Configurations
from kbcstorage.jobs import Jobs
from kbcstorage.tables import Tables
from kbcstorage.tokens import Tokens
from kbcstorage.triggers import Triggers
from kbcstorage.workspaces import Workspaces


class Client:
    """
    Storage API Client.
    """

    def __init__(self, api_domain, token, branch_id='default', file_storage_support=True):
        """
        Initialise a client.

        Args:
            api_domain (str): The domain on which the API sits. eg.
                "https://connection.keboola.com".
            token (str|StorageApiToken|BearerToken): A storage API key, or an
                authentication strategy from `kbcstorage.auth`.
            branch_id (str): The ID of branch to use, use 'default' to work without branch (in main).
            file_storage_support (bool): If False, it saves memory by not importing libraries for all storage backends.
        """
        self.root_url = api_domain.rstrip("/")
        self._auth = coerce(token)
        self._token = self._auth.token
        self._branch_id = branch_id

        self.buckets = Buckets(self.root_url, self.auth)

        if file_storage_support:
            from kbcstorage.files import Files
            self.files = Files(self.root_url, self.auth)

        self.jobs = Jobs(self.root_url, self.auth)
        self.tables = Tables(self.root_url, self.auth)
        self.workspaces = Workspaces(self.root_url, self.auth)
        self.components = Components(self.root_url, self.auth, self.branch_id)
        self.configurations = Configurations(self.root_url, self.auth, self.branch_id)
        self.tokens = Tokens(self.root_url, self.auth)
        self.branches = Branches(self.root_url, self.auth)
        self.triggers = Triggers(self.root_url, self.auth)

    @property
    def token(self):
        return self._token

    @property
    def auth(self):
        return self._auth

    @property
    def project_id(self):
        """The project the requests are scoped to, or None for a storage API token."""
        return getattr(self._auth, 'project_id', None)

    @property
    def branch_id(self):
        return self._branch_id
