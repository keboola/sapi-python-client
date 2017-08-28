import os
import unittest
from requests import exceptions
from kbcstorage.buckets import Buckets


class TestFunctionalBuckets(unittest.TestCase):
    def setUp(self):
        self.buckets = Buckets(os.getenv('KBC_TEST_API_URL'),
                               os.getenv('KBC_TEST_TOKEN'))
        try:
            self.buckets.delete('in.c-py-test', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def tearDown(self):
        try:
            self.buckets.delete('in.c-py-test', force=True)
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    def test_create_bucket(self):
        bucket_id = self.buckets.create(name='py-test',
                                        stage='in',
                                        description='Test bucket')['id']
        self.assertEqual(bucket_id, self.buckets.detail(bucket_id)['id'])

    def test_bucket_detail(self):
        bucket_id = self.buckets.create(name='py-test',
                                        stage='in',
                                        description='Test bucket')['id']
        detail = self.buckets.detail(bucket_id)
        self.assertEqual(bucket_id, detail['id'])
        self.assertEqual('c-py-test', detail['name'])
        self.assertIsNotNone(detail['uri'])
        self.assertIsNotNone(detail['created'])
        self.assertEqual('Test bucket', detail['description'])
        self.assertEqual([], detail['tables'])
        self.assertEqual([], detail['attributes'])

    def test_invalid_bucket(self):
        try:
            self.buckets.detail('some-totally-non-existent-bucket')
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
