import os
import tempfile
import warnings

from requests import exceptions

from base import Endpoint
from configurations import Configurations
from jobs import Jobs
from kbcstorage.triggers import Triggers
from kbcstorage.tables import Tables
from kbcstorage.buckets import Buckets
from tests.base_test_case import BaseTestCase


class Tokens(Endpoint):
    """
    Testing class for obtaining token ID from token.
    """

    def __init__(self, root_url, token):
        super().__init__(root_url, 'tokens', token)

    def verify(self):
        url = '{}/verify'.format(self.base_url)
        return self._get(url)


class TestEndpoint(BaseTestCase):
    TEST_BUCKET_NAME = "kbc_trigger_test_bucket"
    TEST_BUCKET_ID = f"in.c-{TEST_BUCKET_NAME}"
    TEST_TABLE_NAME = "kbc_trigger_test_table"

    def setUp(self):
        self.root_url = os.getenv('KBC_TEST_API_URL')
        self.token = os.getenv('KBC_TEST_TOKEN')

        self.triggers = Triggers(self.root_url, self.token)
        self.tables = Tables(self.root_url, self.token)
        self.buckets = Buckets(self.root_url, self.token)
        self.jobs = Jobs(self.root_url, self.token)
        self.configurations = Configurations(self.root_url, self.token, 'default')
        self.tokens = Tokens(self.root_url, self.token)

        self.created_trigger_ids = []
        # https://github.com/boto/boto3/issues/454
        warnings.simplefilter("ignore", ResourceWarning)

        self.clean()
        self.token_id = self.tokens.verify()["id"]
        self.buckets.create(self.TEST_BUCKET_NAME)
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(b"a,b,c\n1,2,3\n")
        self.table_id = self.tables.create(self.TEST_BUCKET_ID, self.TEST_TABLE_NAME, tmp_file.name)
        self.component = self.TEST_COMPONENT_NAME
        self.configuration_id = self.configurations.create(self.TEST_COMPONENT_NAME, 'trigger_test_config')["id"]

    def clean(self):
        try:
            self.buckets.delete(self.TEST_BUCKET_ID, True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def tearDown(self):
        self.clean()

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
        self.assertTrue(len(self.created_trigger_ids) > 0)
        first_id = self.created_trigger_ids[0]
        detail = self.triggers.detail(first_id)
        self.assertTrue(detail["id"] == first_id)

    def list_triggers(self):
        self.assertTrue(len(self.created_trigger_ids) > 0)
        all_triggers = self.triggers.list()
        api_trigger_ids = {x["id"] for x in all_triggers}
        created_trigger_ids = {x for x in self.created_trigger_ids}
        self.assertTrue(created_trigger_ids.issubset(api_trigger_ids))

    def update_trigger(self):
        self.assertTrue(len(self.created_trigger_ids) > 0)
        first_id = self.created_trigger_ids[0]
        self.triggers.update(first_id, coolDownPeriodMinutes=100)
        detail = self.triggers.detail(first_id)
        self.assertTrue(detail["coolDownPeriodMinutes"] == 100)

    def delete_triggers(self):
        for t_id in self.created_trigger_ids:
            self.triggers.delete(t_id)
