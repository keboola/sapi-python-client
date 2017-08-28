"""
Manages calls to the Storage API relating to tables

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/tables/
"""
from kbcstorage.base import Endpoint
from kbcstorage.files import Files
from kbcstorage.jobs import Jobs


class Tables(Endpoint):
    """
    Buckets Endpoint
    """
    def __init__(self, root_url, token):
        """
        Create a Tables endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'tables', token)

    def list(self, include=None):
        """
        List all tables accessible by token.

        Args:
            include (list): Properties to list (attributes, columns, buckets)
        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = {'X-StorageApi-Token': self.token}

        url = '{}/tables'.format(self.base_url)
        params = {'include': ','.join(include)}
        return self.get(url, headers=headers, params=params)

    def list_bucket(self, bucket_id, include=None):
        """
        List all tables in a bucket.

        Args:
            bucket_id (str): Id of the bucket
            include (list): Properties to list (attributes, columns)
        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = {'X-StorageApi-Token': self.token}

        url = '{}/{}/tables'.format(self.base_url, bucket_id)
        params = {'include': ','.join(include)}
        return self.get(url, headers=headers, params=params)

    def detail(self, table_id):
        """
        Retrieves information about a given table.

        Args:
            table_id (str): The id of the table.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(table_id, str) or table_id == '':
            raise ValueError("Invalid table_id '{}'.".format(table_id))
        url = '{}/{}'.format(self.base_url, table_id)
        headers = {'X-StorageApi-Token': self.token}
        return self.get(url, headers=headers)

    def delete(self, table_id):
        """
        Delete a table referenced by ``table_id``.

        Args:
            table_id (str): The id of the table to be deleted.
        """
        if not isinstance(table_id, str) or table_id == '':
            raise ValueError("Invalid table_id '{}'.".format(table_id))
        url = '{}/{}'.format(self.base_url, table_id)
        headers = {'X-StorageApi-Token': self.token}
        super().delete(url, headers=headers)

    def create(self, bucket_id, name, file_path, delimiter=',', enclosure='"',
               escaped_by='', primary_key=None):
        """
        Create a new table from CSV file.

        Args:
            bucket_id (str): Bucket id where table is created
            name (str): The new table name (only alphanumeric and underscores)
            file_path (str): Path to local CSV file.
            delimiter (str): Field delimiter used in the CSV file.
            enclosure (str): Field enclosure used in the CSV file.
            escaped_by (str): Escape character used in the CSV file.
            primary_key (list): Primary key of a table.

        Returns:
            table_id (str): Id of the created table.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(bucket_id, str) or bucket_id == '':
            raise ValueError("Invalid bucket_id '{}'.".format(bucket_id))
        if not isinstance(name, str) or name == '':
            raise ValueError("Invalid name_id '{}'.".format(name))
        files = Files(self.root_url, self.token)
        file_id = files.upload_file(file_path=file_path, tags=['file-import'],
                                    do_notify=False, is_public=False)
        job = self.create_raw(bucket_id=bucket_id, name=name,
                              data_file_id=file_id, delimiter=delimiter,
                              enclosure=enclosure, escaped_by=escaped_by,
                              primary_key=primary_key)
        jobs = Jobs(self.root_url, self.token)
        job = jobs.block_until_completed(job['id'])
        if job['status'] == 'error':
            raise RuntimeError(job['error']['message'])
        return job['results']['id']

    def create_raw(self, bucket_id, name, data_url=None, data_file_id=None,
                   snapshot_id=None, data_workspace_id=None,
                   data_table_name=None, delimiter=',', enclosure='"',
                   escaped_by='', primary_key=None):
        """
        Create a new table.

        Args:
            bucket_id (str): Bucket id where table is created
            name (str): The new table name (only alphanumeric and underscores)
            data_url (str): Publicly accessible url with a CSV file to import
            data_file_id (str): id of the file stored in File Uploads
            snapshot_id (str): id of a table snapshot -
                a table will be created from the snapshot.
            data_workspace_id (str): Load from the table workspace.
                Use with the dataTableName attribute.
            data_table_name (str): Load from a table in workspace.
            delimiter (str): Field delimiter used in the CSV file.
            enclosure (str): Field enclosure used in the CSV file.
            escaped_by (str): Escape character used in the CSV file.
            primary_key (list): Primary key of a table.

        Returns:
            response_body: The parsed json from the HTTP
                response containing a storage Job.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(bucket_id, str) or bucket_id == '':
            raise ValueError("Invalid bucket_id '{}'.".format(bucket_id))
        if not isinstance(name, str) or name == '':
            raise ValueError("Invalid name_id '{}'.".format(name))
        headers = {
            'X-StorageApi-Token': self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        body = {
            'name': name,
            'delimiter': delimiter,
            'enclosure': enclosure,
            'escapedBy': escaped_by
        }
        body.update(self.validate_data_source(data_url, data_file_id,
                                              snapshot_id, data_workspace_id,
                                              data_table_name))
        if enclosure is not '' and escaped_by is not '':
            raise ValueError("Only one of enclosure and escaped_by may be "
                             "specified.")
        if primary_key is not None and isinstance(primary_key, list):
            body['primaryKey[]'] = primary_key
        # todo solve this better
        url = '{}/v2/storage/buckets/{}/tables-async'.format(self.root_url,
                                                             bucket_id)
        return self.post(url, headers=headers, data=body)

    @staticmethod
    def validate_data_source(data_url, data_file_id, snapshot_id,
                             data_workspace_id, data_table_name):
        """
        Check that table data source is configured properly

        Args:
            data_url (str): Publicly accessible url with a CSV file to import
            data_file_id (str): id of the file stored in File Uploads
            snapshot_id (str): id of a table snapshot - a table will
                be created from the snapshot.
            data_workspace_id (str): Load from the table workspace.
                Use with the dataTableName attribute.
            data_table_name (str): Load from a table in workspace.

        Returns:
            body (dict): Request parameters
        """
        source = False
        body = {}
        if data_url is not None:
            body['dataUrl'] = data_url
            source = True
        if data_file_id is not None:
            if source:
                raise ValueError("Only one of data_url, data_file_id, "
                                 "snapshot_id, data_workspace_id may be "
                                 "specified.")
            body['dataFileId'] = data_file_id
            source = True
        if snapshot_id is not None:
            if source:
                raise ValueError("Only one of data_url, data_file_id, "
                                 "snapshot_id, data_workspace_id may be "
                                 "specified.")
            body['snapshotId'] = snapshot_id
            source = True
        if data_workspace_id is not None and data_table_name is not None:
            if source:
                raise ValueError("Only one of data_url, data_file_id, "
                                 "snapshot_id, data_workspace_id may be "
                                 "specified.")
            body['dataWorkspaceId'] = data_workspace_id
            body['dataTableName'] = data_table_name
            source = True
        if not source:
            raise ValueError("One of data_url, data_file_id, snapshot_id, "
                             "data_workspace_id must be specified.")
        return body

    def load(self, table_id, file_path, is_incremental=False, delimiter=',',
             enclosure='"', escaped_by='', columns=None, without_headers=False):
        """
        Create a new table from CSV file.

        Args:
            table_id (str): Table id
            file_path (str): Path to local CSV file.
            is_incremental (bool): Load incrementally (do not truncate table).
            delimiter (str): Field delimiter used in the CSV file.
            enclosure (str): Field enclosure used in the CSV file.
            escaped_by (str): Escape character used in the CSV file.
            columns (list): List of columns
            without_headers (bool): CSV does not contain headers

        Returns:
            response_body: The parsed json from the HTTP response
                containing write results

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(table_id, str) or table_id == '':
            raise ValueError("Invalid bucket_id '{}'.".format(table_id))
        files = Files(self.root_url, self.token)
        file_id = files.upload_file(file_path=file_path, tags=['file-import'],
                                    do_notify=False, is_public=False)
        job = self.load_raw(table_id=table_id, data_file_id=file_id,
                            delimiter=delimiter, enclosure=enclosure,
                            escaped_by=escaped_by,
                            is_incremental=is_incremental, columns=columns,
                            without_headers=without_headers)
        jobs = Jobs(self.root_url, self.token)
        job = jobs.block_until_completed(job['id'])
        if job['status'] == 'error':
            raise RuntimeError(job['error']['message'])
        return job['results']

    def load_raw(self, table_id, data_url=None, data_file_id=None,
                 snapshot_id=None, data_workspace_id=None, data_table_name=None,
                 is_incremental=False, delimiter=',', enclosure='"',
                 escaped_by='', columns=None, without_headers=False):
        """
        Load data into an existing table

        Args:
            table_id (str): Table id
            data_url (str): Publicly accessible url with a CSV file to import
            data_file_id (str): id of the file stored in File Uploads
            snapshot_id (str): id of a table snapshot - a table will
                be created from the snapshot.
            data_workspace_id (str): Load from the table workspace.
                Use with the dataTableName attribute.
            data_table_name (str): Load from a table in workspace.
            is_incremental (bool): Load incrementally (do not truncate table).
            delimiter (str): Field delimiter used in the CSV file.
            enclosure (str): Field enclosure used in the CSV file.
            escaped_by (str): Escape character used in the CSV file.
            columns (list): List of columns
            without_headers (bool): CSV does not contain headers

        Returns:
            response_body: The parsed json from the HTTP response
                containing a storage Job.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(table_id, str) or table_id == '':
            raise ValueError("Invalid bucket_id '{}'.".format(table_id))
        headers = {
            'X-StorageApi-Token': self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        body = {
            'delimiter': delimiter,
            'enclosure': enclosure,
            'escapedBy': escaped_by,
            'incremental': int(is_incremental),
            'withoutHeaders': int(without_headers)
        }
        body.update(self.validate_data_source(data_url, data_file_id,
                                              snapshot_id, data_workspace_id,
                                              data_table_name))
        if enclosure is not '' and escaped_by is not '':
            raise ValueError("Only one of enclosure and escaped_by may be "
                             "specified.")
        if columns is not None and isinstance(columns, list):
            body['primaryKey[]'] = columns
        url = '{}/{}/import-async'.format(self.base_url, table_id)
        return self.post(url, headers=headers, data=body)

    def preview(self, name, stage='in', description='', backend=None):
        pass

    def export(self, name, stage='in', description='', backend=None):
        pass