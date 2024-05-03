import unittest
import responses
from kbcstorage.triggers import Triggers
from .triggers_responses import list_response, detail_response, create_response, update_response


class TestTriggersWithMocks(unittest.TestCase):
    def setUp(self):
        token = 'dummy_token'
        base_url = 'https://connection.keboola.com/'
        self.triggers = Triggers(base_url, token)

    @responses.activate
    def test_list(self):
        """
        triggers mocks list correctly.
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/triggers',
                json=list_response
            )
        )
        triggers_list = self.triggers.list()
        assert isinstance(triggers_list, list)

    @responses.activate
    def test_detail_by_id(self):
        """
        triggers mocks detail by integer id correctly.
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/triggers/3',
                json=detail_response
            )
        )
        trigger_id = '3'
        trigger_detail = self.triggers.detail(trigger_id)
        assert trigger_detail['id'] == '3'

    @responses.activate
    def test_delete(self):
        """
        Triggers mock deletes trigger by id.
        """
        responses.add(
            responses.Response(
                method='DELETE',
                url='https://connection.keboola.com/v2/storage/triggers/1',
                json={}
            )
        )
        trigger_id = 1
        deleted_detail = self.triggers.delete(trigger_id)
        assert deleted_detail is None

    @responses.activate
    def test_update(self):
        """
        Triggers mock update trigger by id.
        """
        responses.add(
            responses.Response(
                method='PUT',
                url='https://connection.keboola.com/v2/storage/triggers/1',
                json=update_response
            )
        )
        trigger_id = 1
        updated_detail = self.triggers.update(trigger_id, runWithTokenId=100)
        assert updated_detail['id'] == '3'

    @responses.activate
    def test_create(self):
        """
        Triggers mock creates new trigger.
        """
        responses.add(
            responses.Response(
                method='POST',
                url='https://connection.keboola.com/v2/storage/triggers',
                json=create_response
            )
        )
        created_detail = self.triggers.create(
            runWithTokenId=123,
            component="orchestration",
            configurationId=123,
            coolDownPeriodMinutes=20,
            tableIds=[
                "in.c-test.watched-1",
                "in.c-prod.watched-5"
            ]
        )
        assert created_detail['runWithTokenId'] == 123
