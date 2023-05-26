import os
import unittest
from dotenv import load_dotenv


class BaseTestCase(unittest.TestCase):
    TEST_COMPONENT_NAME = 'keboola.runner-config-test'

    @classmethod
    def setUpClass(cls) -> None:
        env_path = os.path.dirname(os.path.abspath(__file__)) + '/../.env'
        load_dotenv(env_path)

        required_envs = ['KBC_TEST_API_URL', 'KBC_TEST_TOKEN', 'SKIP_ABS_TESTS']
        for env in required_envs:
            if os.getenv(env) is None:
                print(env + ' is not configured')
                exit(1)
