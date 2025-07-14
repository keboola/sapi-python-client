"""
Manages calls to the Storage API relating to configurations

Full documentation https://keboola.docs.apiary.io/#reference/components-and-configurations
"""
import json
from kbcstorage.base import Endpoint
from kbcstorage.configurations_metadata import ConfigurationsMetadata


class Configurations(Endpoint):
    """
    Configurations Endpoint
    """

    def __init__(self, root_url, token, branch_id):
        """
        Create a Component endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
            branch_id (str): The ID of branch to use, use 'default' to work without branch (in main).
        """
        super().__init__(root_url, f"branch/{branch_id}/components", token)
        self.metadata = ConfigurationsMetadata(root_url, token, branch_id)

    def detail(self, component_id, configuration_id):
        """
        Retrieves information about a given configuration.

        Args:
            component_id (str): The id of the component.
            configuration_id (str): The id of the configuration.

        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError("Invalid component_id '{}'.".format(component_id))
        if not isinstance(configuration_id, str) or configuration_id == '':
            raise ValueError("Invalid component_id '{}'.".format(configuration_id))
        url = '{}/{}/configs/{}'.format(self.base_url, component_id, configuration_id)
        return self._get(url)

    def delete(self, component_id, configuration_id):
        """
        Deletes the configuration.

        Args:
            component_id (str): The id of the component.
            configuration_id (str): The id of the configuration.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError("Invalid component_id '{}'.".format(component_id))
        if not isinstance(configuration_id, str) or configuration_id == '':
            raise ValueError("Invalid component_id '{}'.".format(configuration_id))
        url = '{}/{}/configs/{}'.format(self.base_url, component_id, configuration_id)
        self._delete(url)

    def list(self, component_id):
        """
        Lists configurations of the given component.

        Args:
            component_id (str): The id of the component.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError("Invalid component_id '{}'.".format(component_id))
        url = '{}/{}/configs'.format(self.base_url, component_id)
        return self._get(url)

    def list_config_workspaces(self, component_id, config_id):
        """
        Lists workspaces for component configuration.

        Args:
            component_id (str): The id of the component.
            config_id (str): The id of the configuration.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError("Invalid component_id '{}'.".format(component_id))
        url = f'{self.base_url}/{component_id}/configs/{config_id}/workspaces'
        return self._get(url)

    def create(self, component_id, name, description='', configuration=None, state=None, change_description='',
               is_disabled=False, configuration_id=None):
        """
        Create a new configuration.

        Args:
            component_id (str): ID of the component to create configuration for.
            name (str): Name of the configuration visible to end-user.
            description (str): Optional configuration description
            configuration (dict): Actual configuration parameters
            state (dict): Optional state parameters
            changeDescription (str): Optional change description
            is_disabled (bool): Optional flag to disable the configuration, default False
            configuration_id (str): Optional configuration ID, if not specified, new ID is generated
        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError("Invalid component_id '{}'.".format(component_id))
        if state is None:
            state = {}
        if configuration is None:
            configuration = {}
        body = {
            'name': name,
            'description': description,
            'configuration': configuration,
            'state': state,
            'changeDescription': change_description,
            'isDisabled': is_disabled
        }
        if configuration_id:
            body['configurationId'] = configuration_id
        url = '{}/{}/configs'.format(self.base_url, component_id)
        return self._post(url, data=json.dumps(body), headers={'Content-Type': 'application/json'})
