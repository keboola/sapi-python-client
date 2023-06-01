"""
Manages calls to the Storage API relating to development branches

Full documentation https://keboola.docs.apiary.io/#reference/development-branches

"""
from kbcstorage.base import Endpoint


class Branches(Endpoint):
    """
    Tokens  Endpoint
    """
    def __init__(self, root_url, token):
        """
        Create a Tokens endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        # Branches have inconsistent endpoint naming - it's either dev-branches or branch, so it need to be resolved
        # endpoint by endpoint.
        super().__init__(root_url, "", token)

    def metadata(self, branch_id="default"):
        """
        Get branch metadata

        Args:
            branch_id (str): The id of the branch or "default" to get metadata for the main branch (production).

        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(branch_id, str) or branch_id == "":
            raise ValueError(f"Invalid branch_id '{branch_id}'")

        url = f"{self.base_url}branch/{branch_id}/metadata"
        return self._get(url)
