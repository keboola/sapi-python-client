import os
import unittest
from requests import exceptions
from kbcstorage.buckets import Buckets


class TestFunctionalBuckets(unittest.TestCase):
    def setUp(self):
        self.buckets = Buckets(os.getenv('KBC_TEST_API_URL') + '/v2/storage/',
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
        self.assertEqual(bucket_id, self.buckets.detail(bucket_id)['id'])
        self.assertEqual('c-py-test', self.buckets.detail(bucket_id)['name'])
        self.assertIsNotNone(self.buckets.detail(bucket_id)['uri'])
        self.assertIsNotNone(self.buckets.detail(bucket_id)['created'])
        self.assertEqual('Test bucket',
                         self.buckets.detail(bucket_id)['description'])
        self.assertEqual([], self.buckets.detail(bucket_id)['tables'])
        self.assertEqual([], self.buckets.detail(bucket_id)['attributes'])

    def test_invalid_bucket(self):
        try:
            self.buckets.detail('some-totally-non-existent-bucket')
        except exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
