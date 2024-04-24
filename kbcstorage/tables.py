"""
Manages calls to the Storage API relating to tables

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/tables/
"""
import tempfile
import os
from kbcstorage.base import Endpoint
from kbcstorage.files import Files
from kbcstorage.jobs import Jobs
from kbcstorage.tables_metadata import TablesMetadata


class Tables(Endpoint):
    """
    Tables Endpoint
    """
    def __init__(self, root_url, token):
        """
        Create a Tables endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'tables', token)
        self.metadata = TablesMetadata(root_url, token)

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
        params = {'include': ','.join(include)} if include else {}
        return self._get(self.base_url, params=params)

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
        return self._get(url)

    def delete(self, table_id):
        """
        Delete a table referenced by ``table_id``.

        Args:
            table_id (str): The id of the table to be deleted.
        """
        if not isinstance(table_id, str) or table_id == '':
            raise ValueError("Invalid table_id '{}'.".format(table_id))
        url = '{}/{}'.format(self.base_url, table_id)
        self._delete(url)

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
        body = {
            'name': name,
            'delimiter': delimiter,
            'enclosure': enclosure,
            'escapedBy': escaped_by
        }
        body.update(self.validate_data_source(data_url, data_file_id,
                                              snapshot_id, data_workspace_id,
                                              data_table_name))
        if enclosure != '' and escaped_by != '':
            raise ValueError("Only one of enclosure and escaped_by may be "
                             "specified.")
        if primary_key is not None and isinstance(primary_key, list):
            body['primaryKey'] = ",".join(primary_key)
        # todo solve this better
        url = '{}/v2/storage/buckets/{}/tables-async'.format(self.root_url,
                                                             bucket_id)
        return self._post(url, data=body)

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
             enclosure='"', escaped_by='', columns=None,
             without_headers=False):
        """
        Load data into an existing table

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
                 snapshot_id=None, data_workspace_id=None,
                 data_table_name=None, is_incremental=False,
                 delimiter=',', enclosure='"', escaped_by='', columns=None,
                 without_headers=False):
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
            raise ValueError("Invalid file_id '{}'.".format(table_id))
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
        if enclosure != '' and escaped_by != '':
            raise ValueError("Only one of enclosure and escaped_by may be "
                             "specified.")
        if columns is not None and isinstance(columns, list):
            body['primaryKey[]'] = columns
        url = '{}/{}/import-async'.format(self.base_url, table_id)
        return self._post(url, data=body)

    @staticmethod
    def validate_filter(where_column, where_operator, where_values):
        params = {}
        if where_column is not None and where_values is not None:
            if not isinstance(where_column, str):
                raise ValueError("Invalid where_column '{}'.".
                                 format(where_column))
            if not isinstance(where_operator, str) or \
                    where_operator not in ('eq', 'neq'):
                raise ValueError("Invalid where_operator '{}'.".
                                 format(where_operator))
            if not isinstance(where_values, list):
                raise ValueError("Invalid where_values '{}'.".
                                 format(where_values))
            params['whereValues[]'] = where_values
            params['whereColumn'] = where_column
            params['whereOperator'] = where_operator
        return params

    def preview(self, table_id, changed_since=None, changed_until=None,
                columns=None, where_column=None, where_values=None,
                where_operator='eq'):
        """
        Export preview of a table.

        Args:
            table_id (str): Table id
            changed_until (str): Filtering by import date
                Both until and since values can be a unix timestamp or any
                date accepted by strtotime.
            changed_since (str): Filtering by import date
                Both until and since values can be a unix timestamp or any
                date accepted by strtotime.
            where_column (str): Column for exporting only matching rows
            where_operator (str): 'eq' or 'neq'
            where_values (list): Values for exporting only matching rows
            columns (list): List of columns to display

        Returns:
            response_body: Table data contents.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        params = {}
        if not isinstance(table_id, str) or table_id == '':
            raise ValueError("Invalid table_id '{}'.".format(table_id))
        if changed_since is not None:
            if not isinstance(changed_since, str):
                raise ValueError("Invalid changed_since '{}'.".
                                 format(changed_since))
            params['changedSince'] = changed_since
        if changed_until is not None:
            if not isinstance(changed_until, str):
                raise ValueError("Invalid changed_until '{}'.".
                                 format(changed_until))
            params['changedUntil'] = changed_until
        params.update(self.validate_filter(where_column, where_operator,
                                           where_values))
        if columns is not None and isinstance(columns, list):
            params['columns'] = ','.join(columns)
        url = '{}/{}/data-preview'.format(self.base_url, table_id)
        r = self._get_raw(url=url, params=params)
        return r.content.decode('utf-8')

    def export_to_file(self, table_id, path_name, limit=None,
                       file_format='rfc', changed_since=None,
                       changed_until=None, columns=None,
                       where_column=None, where_values=None,
                       where_operator='eq', is_gzip=True):
        """
        Export data from a table to a local file

        Args:
            table_id (str): Table id
            path_name (str): Destination path for file.
            limit (int): Number of rows to export.
            file_format (str): 'rfc', 'escaped' or 'raw'
            changed_until (str): Filtering by import date
                Both until and since values can be a unix timestamp or any
                date accepted by strtotime.
            changed_since (str): Filtering by import date
                Both until and since values can be a unix timestamp or any
                date accepted by strtotime.
            where_column (str): Column for exporting only matching rows
            where_operator (str): 'eq' or 'neq'
            where_values (list): Values for exporting only matching rows
            columns (list): List of columns to display
            is_gzip (bool): Result will be gzipped

        Returns:
            destination_file: Local file with exported data

        Raises:
            requests.HTTPError: If the API request fails.
        """

        table_detail = self.detail(table_id)
        job = self.export_raw(table_id=table_id, limit=limit,
                              file_format=file_format,
                              changed_since=changed_since,
                              changed_until=changed_until, columns=columns,
                              where_column=where_column,
                              where_values=where_values,
                              where_operator=where_operator, is_gzip=is_gzip)
        jobs = Jobs(self.root_url, self.token)
        job = jobs.block_until_completed(job['id'])
        if job['status'] == 'error':
            raise RuntimeError(job['error']['message'])
        files = Files(self.root_url, self.token)
        temp_path = tempfile.TemporaryDirectory()
        local_file = files.download(file_id=job['results']['file']['id'],
                                    local_path=temp_path.name)
        destination_file = os.path.join(path_name, table_detail['name'])
        # the file containing table export is always without headers (it is
        # always sliced on Snowflake and Redshift
        if is_gzip:
            import gzip
            import shutil
            with gzip.open(local_file, 'rb') as f_in, \
                    open(local_file + '.un', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(local_file)
            local_file = local_file + '.un'

        with open(local_file, mode='rb') as in_file, \
                open(destination_file, mode='wb') as out_file:
            if columns is None:
                columns = table_detail['columns']
            columns = ['"{}"'.format(col) for col in columns]
            header = ",".join(columns) + '\n'
            out_file.write(header.encode('utf-8'))
            for line in in_file:
                out_file.write(line)
        return destination_file

    def export(self, table_id, limit=None, file_format='rfc',
               changed_since=None, changed_until=None, columns=None,
               where_column=None, where_values=None, where_operator='eq',
               is_gzip=False):
        """
        Export data from a table to a Storage file

        Args:
            table_id (str): Table id
            limit (int): Number of rows to export.
            file_format (str): 'rfc', 'escaped' or 'raw'
            changed_until (str): Filtering by import date
                Both until and since values can be a unix timestamp or any
                date accepted by strtotime.
            changed_since (str): Filtering by import date
                Both until and since values can be a unix timestamp or any
                date accepted by strtotime.
            where_column (str): Column for exporting only matching rows
            where_operator (str): 'eq' or 'neq'
            where_values (list): Values for exporting only matching rows
            columns (list): List of columns to display
            is_gzip (bool): Result will be gzipped

        Returns:
            response_body: File id of the table export

        Raises:
            requests.HTTPError: If the API request fails.
        """

        job = self.export_raw(table_id=table_id, limit=limit,
                              file_format=file_format,
                              changed_since=changed_since,
                              changed_until=changed_until, columns=columns,
                              where_column=where_column,
                              where_values=where_values,
                              where_operator=where_operator, is_gzip=is_gzip)
        jobs = Jobs(self.root_url, self.token)
        job = jobs.block_until_completed(job['id'])
        if job['status'] == 'error':
            raise RuntimeError(job['error']['message'])
        return job['results']['file']['id']

    def export_raw(self, table_id, limit=None, file_format='rfc',
                   changed_since=None, changed_until=None, columns=None,
                   where_column=None, where_values=None, where_operator='eq',
                   is_gzip=False):
        """
        Export data from a table

        Args:
            table_id (str): Table id
            limit (int): Number of rows to export.
            file_format (str): 'rfc', 'escaped' or 'raw'
            changed_until (str): Filtering by import date
                Both until and since values can be a unix timestamp or any
                date accepted by strtotime.
            changed_since (str): Filtering by import date
                Both until and since values can be a unix timestamp or any
                date accepted by strtotime.
            where_column (str): Column for exporting only matching rows
            where_operator (str): 'eq' or 'neq'
            where_values (list): Values for exporting only matching rows
            columns (list): List of columns to display
            is_gzip (bool): Result will be gzipped

        Returns:
            response_body: The parsed json from the HTTP response
                containing a storage Job.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        params = {
            'gzip': int(is_gzip)
        }
        if not isinstance(table_id, str) or table_id == '':
            raise ValueError("Invalid table_id '{}'.".format(table_id))
        if limit is not None and limit is not isinstance(table_id, int):
            raise ValueError("Invalid limit '{}'.".format(limit))
        if file_format not in ('rfc', 'escaped', 'raw'):
            raise ValueError("Invalid format '{}'.".format(file_format))
        if changed_since is not None:
            if not isinstance(changed_since, str):
                raise ValueError("Invalid changed_since '{}'.".
                                 format(changed_since))
            params['changedSince'] = changed_since
        if changed_until is not None:
            if not isinstance(changed_until, str):
                raise ValueError("Invalid changed_until '{}'.".
                                 format(changed_until))
            params['changedUntil'] = changed_until
        params.update(self.validate_filter(where_column, where_operator,
                                           where_values))
        if columns is not None and isinstance(columns, list):
            params['columns'] = ','.join(columns)
        url = '{}/{}/export-async'.format(self.base_url, table_id)
        return self._post(url, data=params)

    def optimize(self, table_id):
        """Optimize RedShift table size

        http://docs.keboola.apiary.io/#reference/tables/table-optimize/optimize-table

        Args:
            table_id (str): table id to optimize ("in.c-my-bucket.table666")

        Returns:
            json object with optimization statistics. The optimization
            happens asynchronously. You can use the 'id' parameter from the
            response body to poll the status of the job using methods
            implemented in Client.Jobs endpoint. (detail, block_for_success,
            block_until_completed, etc...)

        """
        url = '{}/{}/optimize'.format(self.base_url, table_id)
        return self._post(url)
