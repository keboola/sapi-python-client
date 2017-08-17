from kbcstorage.client import Client
import os
import unittest
from requests import exceptions


class TestBuckets(unittest.TestCase):
    def setUp(self):
        self.client = Client(os.getenv('KBC_TEST_TOKEN'), os.getenv('KBC_TEST_API_URL'))
        try:
            self.client.drop_bucket('in.c-py-test', {'force': True})
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def tearDown(self):
        try:
            self.client.drop_bucket('in.c-py-test', {'force': True})
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_create_bucket(self):
        bucket_id = self.client.create_bucket('py-test', 'in', 'Test bucket')
        self.assertTrue(self.client.bucket_exists(bucket_id))

    def test_bucket_exists(self):
        bucket_id = self.client.create_bucket('py-test', 'in', 'Test bucket')
        self.assertTrue(self.client.bucket_exists(bucket_id))
        self.assertFalse(self.client.bucket_exists('some-totally-non-existent-bucket'))
