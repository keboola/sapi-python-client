"""
Manages calls to the Storage API relating to workspaces.

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/workspaces/
"""
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from kbcstorage.base import Endpoint
from kbcstorage.files import Files
from kbcstorage.jobs import Jobs
from kbcstorage.tokens import Tokens
from typing import List  # the legacy Workspaces class below unfortunately defines its own method called list


BACKEND_SNOWFLAKE = 'snowflake'
LOGIN_TYPE_DEFAULT = 'default'
LOGIN_TYPE_SNOWFLAKE_SERVICE_KEYPAIR = 'snowflake-service-keypair'


def _generate_rsa_key_pair():
    """
    Generate an RSA-2048 key pair for Snowflake key-pair authentication.

    Returns:
        (private_key_pem, public_key_pem): Both keys PEM-encoded, the private
            key in PKCS#8 format as expected by Snowflake drivers.
    """
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode('ascii')
    public_key_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode('ascii')
    return private_key_pem, public_key_pem


def _make_body(mapping, source_key='source', preserve: bool = True):
    """
    Given a dict mapping Keboola tables to aliases, construct the body of
    the HTTP request to load said tables.

    Args:
        mapping(:obj:`dict`): Keys contain the full names of the tables to
            be loaded (ie. 'in.c-bucker.table_name') and values contain the
            aliases to which they will be loaded (ie. 'table_name').
    """
    body = {'preserve': str(preserve).lower()}
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

    def create(self, backend=None, timeout=None, login_type=None, public_key=None, read_all_objects=False):
        """
        Create a new Workspace and return the credentials.

        On the snowflake backend, an omitted (or 'default') login_type would
        create a deprecated password-based workspace. The client therefore
        defaults to the 'snowflake-service-keypair' login type: when no
        public_key is supplied, an RSA key pair is generated locally, the
        public key is sent to the API and the private key is returned in
        response['connection']['privateKey'] (it never leaves the client
        otherwise and cannot be retrieved later). To get the deprecated
        password-based workspace, pass login_type='snowflake-legacy-service'
        explicitly.

        Args:
            backend (:obj:`str`): The type of engine for the workspace.
                'redshift', 'snowflake' or 'synapse'. Defaults to the project's default backend.
            timeout (int): The timeout, in seconds, for SQL statements.
                Only supported by snowflake backends.
            login_type (:obj:`str`): The login type of the workspace, e.g.
                'snowflake-service-keypair', 'snowflake-person-keypair',
                'snowflake-legacy-service' or 'none'. Defaults to
                'snowflake-service-keypair' on snowflake, otherwise to the
                backend's default.
            public_key (:obj:`str`): PEM-encoded RSA public key to use with
                key-pair login types. When omitted for the default snowflake
                key-pair login, a key pair is generated locally.
            read_all_objects (bool): Grant the workspace read-only access to
                all project data.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        private_key = None
        effective_backend = backend or self._get_default_backend()
        if effective_backend == BACKEND_SNOWFLAKE:
            if login_type in (None, LOGIN_TYPE_DEFAULT):
                login_type = LOGIN_TYPE_SNOWFLAKE_SERVICE_KEYPAIR
            if login_type == LOGIN_TYPE_SNOWFLAKE_SERVICE_KEYPAIR and public_key is None:
                private_key, public_key = _generate_rsa_key_pair()
        if login_type is not None:
            # the API rejects loginType without an explicit backend
            backend = effective_backend

        body = {
            'backend': backend,
            'statementTimeoutSeconds': timeout,
            'loginType': login_type,
            'publicKey': public_key,
            'readOnlyStorageAccess': str(read_all_objects).lower()  # convert bool to lowercase true or false
        }

        response = self._post(self.base_url, data=body)
        if private_key is not None:
            response.setdefault('connection', {})['privateKey'] = private_key
        return response

    def _get_default_backend(self):
        """
        Resolve the project's default backend from the token detail.
        """
        token_info = Tokens(self.root_url, self.token).verify()
        return (token_info.get('owner') or {}).get('defaultBackend')

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

    def set_public_key(self, workspace_id, public_key):
        """
        Set the public key for the workspace.
        """
        data = {
            'publicKey': public_key
        }
        url = '{}/{}/public-key'.format(self.base_url, workspace_id)
        return self._post(url, json=data)

    def load_tables(self, workspace_id: int | str, table_mapping: dict | List[dict], preserve=True, load_type='load'):
        """
        Load tabes from storage into a workspace.

        Args:
            workspace_id (int or str): The id of the workspace to which to load
                the tables.
            table_mapping (:obj:`dict` or :obj:`list`): Source table names mapped to
                destination table names. or a list of dicts with detailed tables specification.
            preserve (bool): If False, drop tables, else keep tables in
                workspace.
            load_type (str): Type of load, either 'load' or 'load-clone'. Defaults to 'load'.

        Raises:
            requests.HTTPError: If the API request fails.

        Todo:
            * Column data types.
        """
        load_type = load_type.lower()
        if load_type not in ['load', 'load-clone']:
            raise ValueError("Invalid load_type: {}, supports only load and load-clone".format(load_type))

        url = "/".join([self.base_url, str(workspace_id), load_type])

        req = None
        if isinstance(table_mapping, dict):
            body = _make_body(table_mapping, preserve=preserve)
            req = self._post(url, data=body)
        elif isinstance(table_mapping, list):
            body = {'input': table_mapping, 'preserve': str(preserve).lower()}
            req = self._post(url, json=body)

        return req

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
