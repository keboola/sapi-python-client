"""
Manages calls to the Storage API relating to configurations

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/tables/
"""
import tempfile
import os
from kbcstorage.base import Endpoint
from kbcstorage.files import Files
from kbcstorage.jobs import Jobs


class Components(Endpoint):
    """
    Components Endpoint
    """
    def __init__(self, root_url, token):
        """
        Create a Component endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'branch/default/components', token)

    def detail(self, component_id, configuration_id):
        """
        Retrieves information about a given configuration.

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
        return self._get(url)
