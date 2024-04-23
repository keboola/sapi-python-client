"""
Manages calls to the Storage API relating to configurations metadata

Full documentation https://keboola.docs.apiary.io/#reference/metadata/components-configurations-metadata/
"""
import json
from kbcstorage.base import Endpoint


class ConfigurationsMetadata(Endpoint):
    """
    Configurations metadata Endpoint
    """

    def __init__(self, root_url, token, branch_id):
        """
        Create a Component metadata endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
            branch_id (str): The ID of branch to use, use 'default' to work without branch (in main).
        """
        super().__init__(root_url, f"branch/{branch_id}/components", token)

    def delete(self, component_id, configuration_id, metadata_id):
        """
        Deletes the configuration metadata identified by ``metadata_id``.

        Args:
            component_id (str): The id of the component.
            configuration_id (str): The id of the configuration.
            metadata_id (str): The id of the metadata (not key!).

        Raises:
            requests.HTTPError: If the API request fails.
            ValueError: If the component_id/configuration_id/metadata_id is not a string or is empty.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError("Invalid component_id '{}'.".format(component_id))
        if not isinstance(configuration_id, str) or configuration_id == '':
            raise ValueError("Invalid configuration_id '{}'.".format(configuration_id))
        if not isinstance(metadata_id, str) or metadata_id == '':
            raise ValueError("Invalid metadata_id '{}'.".format(metadata_id))
        url = '{}/{}/configs/{}/metadata/{}'.format(self.base_url, component_id, configuration_id, metadata_id)
        self._delete(url)

    def list(self, component_id, configuration_id):
        """
        Lists metadata for a given component configuration.

        Args:
            component_id (str): The id of the component.
            configuration_id (str): The id of the configuration.

        Raises:
            requests.HTTPError: If the API request fails.
            ValueError: If the component_id/configuration_id is not a string or is empty.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError("Invalid component_id '{}'.".format(component_id))
        if not isinstance(configuration_id, str) or configuration_id == '':
            raise ValueError("Invalid configuration_id '{}'.".format(configuration_id))
        url = '{}/{}/configs/{}/metadata'.format(self.base_url, component_id, configuration_id)
        return self._get(url)

    def create(self, component_id, configuration_id, provider, metadata):
        """
        Writes metadata for a given component configuration.

        Args:
            component_id (str): The id of the component.
            configuration (str): The id of the configuration.
            provider (str): The provider of the configuration (currently ignored and "user" is sent).
            metadata (list): A list of metadata items. Item is a dictionary with 'key' and 'value' keys.

        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
            ValueError: If the component_id/configuration_id is not a string or is empty.
            ValueError: If the metadata is not a list.
            ValueError: If the metadata item is not a dictionary.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError("Invalid component_id '{}'.".format(component_id))
        if not isinstance(configuration_id, str) or configuration_id == '':
            raise ValueError("Invalid component_id '{}'.".format(configuration_id))
        url = '{}/{}/configs/{}/metadata'.format(self.base_url, component_id, configuration_id)
        if not isinstance(metadata, list):
            raise ValueError("Metadata must be a list '{}'.".format(metadata))
        for metadataItem in metadata:
            if not isinstance(metadataItem, dict):
                raise ValueError("Metadata item must be a dictionary '{}'.".format(metadataItem))

        headers = {
              'Content-Type': 'application/json',
        }
        data = {
            # 'provider': provider, # not yet implemented
            'metadata': metadata
        }
        return self._post(url, data=json.dumps(data), headers=headers)
