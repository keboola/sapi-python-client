"""
Manages calls to the Storage API relating to triggers

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/triggers/
"""
from kbcstorage.base import Endpoint


class Triggers(Endpoint):
    """
    Triggers Endpoint
    """

    def __init__(self, root_url, token):
        """
        Create a Triggers endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'triggers', token)

    def list(self):
        """
        List all triggers in project.

        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """

        return self._get(self.base_url)

    def detail(self, trigger_id):
        """
        Retrieves information about a given trigger.

        Args:
            trigger_id (str): The id of the trigger.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}'.format(self.base_url, trigger_id)

        return self._get(url)

    def create(self, runWithTokenId, component, configurationId, coolDownPeriodMinutes, tableIds):
        """
        Create a new trigger.

        Args:
            runWithTokenId (int): ID of token used for running configured component.
            component (str): For now we support only 'orchestration'.
            configurationId (int): Id of component configuration.
            coolDownPeriodMinutes (int): Minimal cool down period before
                firing action again in minutes (min is 1 minute).
            tableIds (list[str]) IDs of tables.
        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        # Separating create and link into two distinct functions...
        # Need to check args...
        body = {
            "runWithTokenId": runWithTokenId,
            "component": component,
            "configurationId": configurationId,
            "coolDownPeriodMinutes": coolDownPeriodMinutes,
            "tableIds": tableIds
        }

        return self._post(self.base_url, json=body)

    def delete(self, trigger_id):
        """
        Delete a trigger referenced by ``trigger_id``.

        Args:
            trigger_id (int): The id of the trigger to be deleted.

        """
        url = '{}/{}'.format(self.base_url, trigger_id)
        self._delete(url)

    def update(self, trigger_id, runWithTokenId=None, component=None, configurationId=None,
               coolDownPeriodMinutes=None, tableIds=None):
        """
        Update a trigger referenced by ``trigger_id``.

        Args:
            runWithTokenId (int): ID of token used for running configured component.
            component (str): For now we support only 'orchestration'.
            configurationId (int): Id of component configuration.
            coolDownPeriodMinutes (int): Minimal cool down period before
                firing action again in minutes (min is 1 minute).
            tableIds (list[str]) IDs of tables.
        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}'.format(self.base_url, trigger_id)
        body = {
            k: v for k, v in {
                "runWithTokenId": runWithTokenId,
                "component": component,
                "configurationId": configurationId,
                "coolDownPeriodMinutes": coolDownPeriodMinutes,
                "tableIds": tableIds
            }.items()
            if v is not None
        }
        return self._put(url, data=body)
