"""
Manages calls to the Storage API relating to table metadatas

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/metadata/table-metadata
"""
import json
from kbcstorage.base import Endpoint


class TablesMetadata(Endpoint):
    """
    Tables Metadata Endpoint
    """
    def __init__(self, root_url, token):
        """
        Create a Tables endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'tables', token)

    def list(self, table_id):
        """
        List all metadata for table

        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
            ValueError: If the table_id is not a string or is empty.
        """
        if not isinstance(table_id, str) or table_id == '':
            raise ValueError("Invalid table_id '{}'.".format(table_id))

        url = '{}/{}/metadata'.format(self.base_url, table_id)

        return self._get(url)

    def delete(self, table_id, metadata_id):
        """
        Delete a table referenced by ``table_id``.

        Args:
            table_id (str): The id of the table to be deleted.

        Raises:
            requests.HTTPError: If the API request fails.
            ValueError: If the table_id is not a string or is empty.
        """
        if not isinstance(table_id, str) or table_id == '':
            raise ValueError("Invalid table_id '{}'.".format(table_id))
        if not isinstance(metadata_id, str) or metadata_id == '':
            raise ValueError("Invalid metadata_id '{}'.".format(metadata_id))

        url = '{}/{}/metadata/{}'.format(self.base_url, table_id, metadata_id)

        self._delete(url)

    def create(self, table_id, provider, metadata, columns_metadata):
        """
        Post metadata to a table.

        Args:
            table_id (str): Table id
            provider (str): Provider of the metadata
            metadata (list): List of metadata dictionaries with 'key' and 'value'
            columns_metadata (dict): Dictionary with column metadata

        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(table_id, str) or table_id == '':
            raise ValueError("Invalid table_id '{}'.".format(table_id))
        if not isinstance(provider, str) or provider == '':
            raise ValueError("Invalid provider '{}'.".format(provider))
        if not isinstance(metadata, list):
            raise ValueError("Invalid metadata '{}'.".format(metadata))
        if not isinstance(columns_metadata, dict):
            raise ValueError("Invalid columns_metadata '{}'.".format(columns_metadata))

        url = '{}/{}/metadata'.format(self.base_url, table_id)
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            "provider": provider,
            "metadata": metadata,
            "columnsMetadata": columns_metadata
        }
        return self._post(url, data=json.dumps(data), headers=headers)
