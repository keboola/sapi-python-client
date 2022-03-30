"""
Manages calls to the Storage API relating to components.

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/components/
"""

import json
import urllib

from kbcstorage.base import Endpoint


class Components(Endpoint):
    """

    """

    def __init__(self, root_url, token):
        """
        Create a Components endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'components', token)

    def list(self):
        """
        List all components details.

        Returns:
            response_body: The json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        return self._get(self.base_url)

    def detail(self, component_id):
        """
        Retrieves information about a given component.

        Args:
            component_id (str or int): The id of the component.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}'.format(self.base_url, component_id)

        return self._get(url)

    def get_config_detail(self, component_id, configuration_id):
        """
        Retrieves component's configuration detail.

        Args:
            component_id (str or int): The id of the component.
            configuration_id (str or int): The id of configuration
        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}/configs/{}'.format(self.base_url, component_id, configuration_id)

        return self._get(url)

    def get_config_row_detail(self, component_id, configuration_id, row_id):
        """
        Retrieves component's configuration row detail.

        Args:
            component_id (str or int): The id of the component.
            configuration_id (str or int): The id of configuration
            row_id
        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}/configs/{}/rows/{}'.format(self.base_url, component_id, configuration_id, row_id)

        return self._get(url)

    def get_config_rows(self, component_id, configuration_id):
        """
        Retrieves component's configuration detail.

        Args:
            component_id (str or int): The id of the component.
            configuration_id (int): The id of configuration
        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}/configs/{}/rows'.format(self.base_url, component_id, configuration_id)

        return self._get(url)

    def update_configuration(self, component_id, configuration_id, configuration, name, state=None, description='',
                             change_description='', row_sort_order=None):
        """

        Args:
            component_id: 
            name: required string Configuration name
            description: string Configuration description
            configuration: dict configuration JSON; the maximum allowed size is 4MB
            state: state dict configuration state JSON; the maximum allowed size is 4MB
            change_description: string Description of the configuration modification
            row_sort_order:

            param component_id:
            configuration_id:

        Returns:

        """
        parameters = {}
        url = '{}/{}/configs/{}'.format(self.base_url, component_id, configuration_id)
        # convert objects to string
        parameters['configuration'] = json.dumps(configuration)
        parameters['name'] = name
        parameters['description'] = description
        parameters['changeDescription'] = change_description
        parameters['rowsSortOrder'] = row_sort_order
        if state:
            parameters['state'] = json.dumps(state)

        return self._put(url, data=parameters)

    def create(self, component_id, name, description, configuration, configurationId=None, state=None,
               changeDescription='', **kwargs):
        """
        Create a new table from CSV file.

        Args:
            component_id (str):
            name (str): The new table name (only alphanumeric and underscores)
            configuration (dict): configuration JSON; the maximum allowed size is 4MB
            state (dict): configuration JSON; the maximum allowed size is 4MB
            changeDescription (str): Escape character used in the CSV file.

        Returns:
            table_id (str): Id of the created table.

        Raises:
            requests.HTTPError: If the API request fails.
        """

        parameters = {}
        url = '{}/{}/configs'.format(self.base_url, component_id)
        # convert objects to string
        if configurationId:
            parameters['configurationId'] = configurationId
        parameters['configuration'] = json.dumps(configuration)
        parameters['name'] = name
        parameters['description'] = description
        parameters['changeDescription'] = changeDescription
        if state:
            parameters['state'] = json.dumps(state)
        header = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = urllib.parse.urlencode(parameters)
        return self._post(url, data=data, headers=header)

    def create_config_row(self, component_id, configuration_id, name, configuration,
                          description='', row_id=None, state=None, change_description='', is_disabled=False, **kwargs):
        """
        Create a new table from CSV file.

        Args:
            row_id:
            description:
            is_disabled:
            component_id (str):
            configuration_id (str):
            name (str): The new table name (only alphanumeric and underscores)
            configuration (dict): configuration JSON; the maximum allowed size is 4MB
            state (dict): configuration JSON; the maximum allowed size is 4MB
            change_description (str): Escape character used in the CSV file.

        Returns:
            table_id (str): Id of the created table.

        Raises:
            requests.HTTPError: If the API request fails.
        """

        parameters = {}
        url = url = '{}/{}/configs/{}/rows'.format(self.base_url, component_id, configuration_id)
        # convert objects to string
        parameters['configuration'] = json.dumps(configuration)
        parameters['name'] = name
        parameters['description'] = description
        if row_id:
            parameters['rowId'] = row_id
        parameters['changeDescription'] = change_description
        parameters['isDisabled'] = is_disabled
        if state:
            parameters['state'] = json.dumps(state)

        header = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = urllib.parse.urlencode(parameters)
        return self._post(url, data=data, headers=header)

    def update_config_row(self, component_id, configuration_id, row_id, name, configuration,
                          description='', rowId=None, state=None, changeDescription='', isDisabled=False, **kwargs):
        """
        Create a new table from CSV file.

        Args:
            component_id (str):
            name (str): The new table name (only alphanumeric and underscores)
            configuration (dict): configuration JSON; the maximum allowed size is 4MB
            state (dict): configuration JSON; the maximum allowed size is 4MB
            changeDescription (str): Escape character used in the CSV file.

        Returns:
            table_id (str): Id of the created table.

        Raises:
            requests.HTTPError: If the API request fails.
        """

        parameters = {}
        url = '{}/{}/configs/{}'.format(self.base_url, component_id, configuration_id)
        # convert objects to string
        parameters['configuration'] = json.dumps(configuration)
        parameters['name'] = name
        parameters['description'] = description
        if rowId:
            parameters['rowId'] = rowId
        parameters['changeDescription'] = changeDescription
        parameters['isDisabled'] = isDisabled
        if state:
            parameters['state'] = json.dumps(state)

        header = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = urllib.parse.urlencode(parameters)
        return self._put(url, data=data, headers=header)

    def get_all_component_configurations(self, component_id, include: str = 'configuration,rows,state'):
        """
        Get all component configurations
        Args:
            component_id:
            include:

        Returns:

        """
        parameters = {}
        url = f'{self.base_url}/{component_id}/configs'

        if include:
            parameters['include'] = include

        return self._get(url, params=parameters)
