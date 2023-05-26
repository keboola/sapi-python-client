"""
Manages calls to the Storage API relating to components

Full documentation https://keboola.docs.apiary.io/#reference/components-and-configurations
"""
from kbcstorage.base import Endpoint


class Components(Endpoint):
    """
    Components Endpoint
    """
    def __init__(self, root_url, token, branch_id):
        """
        Create a Configuration endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
            branch_id (str): The ID of branch to use, use 'default' to work without branch (in main).
        """
        super().__init__(root_url, f"branch/{branch_id}/components", token)

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
        params = {'include': ',' . join(include)} if include else {}
        return self._get(self.base_url, params=params)
