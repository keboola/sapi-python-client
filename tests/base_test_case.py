import os
import unittest
from dotenv import load_dotenv


class BaseTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        env_path = os.path.dirname(os.path.abspath(__file__)) + '/../.env'
        print(env_path)
        load_dotenv(env_path)

        required_envs = ['KBC_TEST_API_URL', 'KBC_TEST_TOKEN', 'SKIP_ABS_TESTS']
        for env in required_envs:
            if os.getenv(env) is None:
                print(env + ' is not configured')
                exit(1)

        print('All required envs are configured')
