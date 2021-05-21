"""
Manages calls to the Storage API relating to workspaces.

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/workspaces/
"""
from kbcstorage.base import Endpoint
from kbcstorage.files import Files
from kbcstorage.jobs import Jobs


def _make_body(mapping, source_key='source'):
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
        body[template.format(i, source_key)] = k
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
        return self._get(self.base_url)

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
        url = '{}/{}'.format(self.base_url, workspace_id)
        return self._get(url)

    def create(self, backend=None, timeout=None):
        """
        Create a new Workspace and return the credentials.

        Args:
            backend (:obj:`str`): The type of engine for the workspace.
                'redshift', 'snowflake' or 'synapse'. Defaults to the project's default backend.
            timeout (int): The timeout, in seconds, for SQL statements.
                Only supported by snowflake backends.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        body = {
            'backend': backend,
            'statementTimeoutSeconds': timeout
        }

        return self._post(self.base_url, data=body)

    def delete(self, workspace_id):
        """
        Deletes a workspace.

        This also irreversibly removes workspace content.

        Args:
            workspace_id (int or str): The id of the workspace to be deleted.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}'.format(self.base_url, workspace_id)

        self._delete(url)

    def reset_password(self, workspace_id):
        """
        Generate a new password for the workspace.

        Args:
            workspace_id (int or str): The id of the workspace for which the
                password should be reset.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}/password'.format(self.base_url, workspace_id)
        return self._post(url)

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
        body = _make_body(table_mapping)
        body['preserve'] = preserve
        url = '{}/{}/load'.format(self.base_url, workspace_id)

        return self._post(url, data=body)

    def load_files(self, workspace_id, file_mapping):
        """
        Load files from file storage into a workspace.
        * only supports abs workspace
        writes the matching files to "{destination}/file_name/file_id"

        Args:
            workspace_id (int or str): The id of the workspace to which to load
                the tables.
            file_mapping (:obj:`dict`):
                tags: [],
                operator: enum('or', 'and') default or,
                destination: string path without trailing /

        Raises:
            requests.HTTPError: If the API request fails.
        """
        workspace = self.detail(workspace_id)
        if (workspace['type'] != 'file' and workspace['connection']['backend'] != 'abs'):
            raise Exception('Loading files to workspace is only available for ABS workspaces')
        files = Files(self.root_url, self.token)
        if ('operator' in file_mapping and file_mapping['operator'] == 'and'):
            query = ' AND '.join(map(lambda tag: 'tags:"' + tag + '"', file_mapping['tags']))
            file_list = files.list(q=query)
        else:
            file_list = files.list(tags=file_mapping['tags'])

        jobs = Jobs(self.root_url, self.token)
        jobs_list = []
        for file in file_list:
            inputs = {
                file['id']: "%s/%s" % (file_mapping['destination'], file['name'])
            }
            body = _make_body(inputs, source_key='dataFileId')
            # always preserve the workspace, otherwise it would be silly
            body['preserve'] = 1
            url = '{}/{}/load'.format(self.base_url, workspace['id'])
            job = self._post(url, data=body)
            jobs_list.append(job)

        for job in jobs_list:
            if not (jobs.block_for_success(job['id'])):
                try:
                    print("Failed to load a file with error: %s" % job['results']['message'])
                except IndexError:
                    print("An unknown error occurred loading data.  Job ID %s" % job['id'])
