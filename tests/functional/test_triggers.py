import os
import warnings

from kbcstorage.triggers import Triggers
from tests.base_test_case import BaseTestCase


class TestEndpoint(BaseTestCase):
    def setUp(self):
        self.triggers = Triggers(os.getenv('KBC_TEST_API_URL'), os.getenv('KBC_TEST_TOKEN'))
        required_envs = ['KBC_TOKEN_ID', 'KBC_COMPONENT', 'KBC_CONFIGURATION_ID', 'KBC_TABLE_ID']
        for env in required_envs:
            if os.getenv(env) is None:
                print(env + ' is not configured')
                exit(1)
        self.token_id = int(os.getenv("KBC_TOKEN_ID"))
        self.component = os.getenv("KBC_COMPONENT")
        self.table_id = os.getenv("KBC_TABLE_ID")
        self.configuration_id = int(os.getenv("configurationId"))

        self.created_trigger_ids = []
        # https://github.com/boto/boto3/issues/454
        warnings.simplefilter("ignore", ResourceWarning)

    def test_steps(self):
        self.create_trigger()
        self.list_triggers()
        self.trigger_detail()
        self.update_trigger()
        self.delete_triggers()

    def create_trigger(self):
        trigger_id = self.triggers.create(
            runWithTokenId=self.token_id,
            component=self.component,
            configurationId=self.configuration_id,
            coolDownPeriodMinutes=10,
            tableIds=[self.table_id]
        )['id']
        self.created_trigger_ids.append(trigger_id)
        self.assertEqual(trigger_id, self.triggers.detail(trigger_id)['id'])

    def trigger_detail(self):
        assert len(self.created_trigger_ids) > 0
        first_id = self.created_trigger_ids[0]
        detail = self.triggers.detail(first_id)
        assert detail["id"] == first_id

    def list_triggers(self):
        assert len(self.created_trigger_ids) > 0
        all_triggers = self.triggers.list()
        api_trigger_ids = {x["id"] for x in all_triggers}
        created_trigger_ids = {x for x in self.created_trigger_ids}
        assert created_trigger_ids.issubset(api_trigger_ids)

    def update_trigger(self):
        assert len(self.created_trigger_ids) > 0
        first_id = self.created_trigger_ids[0]
        self.triggers.update(first_id, coolDownPeriodMinutes=100)
        detail = self.triggers.detail(first_id)
        assert detail["coolDownPeriodMinutes"] == 100

    def delete_triggers(self):
        for t_id in self.created_trigger_ids:
            self.triggers.delete(t_id)
