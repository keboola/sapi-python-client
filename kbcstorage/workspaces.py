"""
Manages calls to the Storage API relating to workspaces.

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/workspaces/
"""
from kbcstorage.base import Endpoint


def _make_body(mapping):
    """
    Given a dict mapping Keboola tables to aliases, construct the body of
    the HTTP request to load said tables.

    Args:
        mapping(:obj:`dict`): Keys contain the full names of the tables to
            be loaded (ie. 'in.c-bucker.table_name') and values contain the
            aliases to which they will be loaded (ie. 'table_name').
    """
    body = {}
    template = 'input[{0}][{1}]'
    for i, (k, v) in enumerate(mapping.items()):
        body[template.format(i, 'source')] = k
        body[template.format(i, 'destination')] = v

    return body


class Workspaces(Endpoint):
    """
    Workspaces Endpoint
    """
    def __init__(self, root_url, token):
        """
        Create a Workspaces endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'workspaces', token)

    def list(self):
        """
        List the details of all workspaces in the project.

        Returns:
            response_body: The json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = {'X-StorageApi-Token': self.token}
        return self._get(self.base_url, headers=headers)

    def detail(self, workspace_id):
        """
        Retrieves information about a given workspace.

        Note that the password to the workspace can only be retrieved when the
        workspace is created.

        Args:
            workspace_id (int or str): The id of the workspace.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = {'X-StorageApi-Token': self.token}
        url = '{}/{}'.format(self.base_url, workspace_id)
        return self._get(url, headers=headers)

    def create(self, backend=None, timeout=None):
        """
        Create a new Workspace and return the credentials.

        Args:
            backend (:obj:`str`): The type of engine for the workspace.
                'redshift' or 'snowflake'. Default redshift.
            timeout (int): The timeout, in seconds, for SQL statements.
                Only supported by snowflake backends.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = {
            'X-StorageApi-Token': self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        body = {
            'backend': backend,
            'statementTimeoutSeconds': timeout
        }

        return self._post(self.base_url, data=body, headers=headers)

    def delete(self, workspace_id):
        """
        Deletes a workspace.

        This also irreversibly removes workspace content.

        Args:
            workspace_id (int or str): The id of the workspace to be deleted.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = {
            'X-StorageApi-Token': self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        url = '{}/{}'.format(self.base_url, workspace_id)

        self._delete(url, headers=headers)

    def reset_password(self, workspace_id):
        """
        Generate a new password for the workspace.

        Args:
            workspace_id (int or str): The id of the workspace for which the
                password should be reset.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = {
            'X-StorageApi-Token': self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        url = '{}/{}/password'.format(self.base_url, workspace_id)
        return self._post(url, headers=headers)

    def load_tables(self, workspace_id, table_mapping, preserve=None):
        """
        Load tabes from storage into a workspace.

        Args:
            workspace_id (int or str): The id of the workspace to which to load
                the tables.
            table_mapping (:obj:`dict`): Source table names mapped to
                destination table names.
            preserve (bool): If False, drop tables, else keep tables in
                workspace.

        Raises:
            requests.HTTPError: If the API request fails.

        Todo:
            * Column data types.
        """
        headers = {
            'X-StorageApi-Token': self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        body = _make_body(table_mapping)
        body['preserve'] = preserve
        url = '{}/{}/load'.format(self.base_url, workspace_id)

        return self._post(url, data=body, headers=headers)
