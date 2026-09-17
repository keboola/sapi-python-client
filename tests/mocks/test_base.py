import unittest

import responses

from kbcstorage.auth import BearerToken
from kbcstorage.base import Endpoint

BASE_URL = 'https://connection.keboola.com/'
URL = 'https://connection.keboola.com/v2/storage/dummy'


def _mock_get():
    responses.add(responses.Response(method='GET', url=URL, json={}))


class TestEndpointAuthHeaders(unittest.TestCase):
    @responses.activate
    def test_storage_api_token_headers(self):
        _mock_get()
        Endpoint(BASE_URL, 'dummy', 'dummy_token')._get(URL)

        headers = responses.calls[0].request.headers
        self.assertEqual('dummy_token', headers['X-StorageApi-Token'])
        self.assertNotIn('Authorization', headers)
        self.assertNotIn('X-KBC-ProjectId', headers)

    @responses.activate
    def test_bearer_token_headers(self):
        _mock_get()
        Endpoint(BASE_URL, 'dummy', BearerToken('kbc_at_dummy', 1234))._get(URL)

        headers = responses.calls[0].request.headers
        self.assertEqual('Bearer kbc_at_dummy', headers['Authorization'])
        self.assertEqual('1234', headers['X-KBC-ProjectId'])
        self.assertNotIn('X-StorageApi-Token', headers)

    @responses.activate
    def test_common_headers_sent_with_both_strategies(self):
        for token in ('dummy_token', BearerToken('kbc_at_dummy', 1234)):
            with self.subTest(token=token):
                responses.reset()
                _mock_get()
                Endpoint(BASE_URL, 'dummy', token)._get(URL)

                headers = responses.calls[0].request.headers
                self.assertEqual('gzip', headers['Accept-Encoding'])
                self.assertEqual('Keboola Storage API Python Client', headers['User-Agent'])

    def test_token_attribute_is_the_raw_token(self):
        self.assertEqual('dummy_token', Endpoint(BASE_URL, 'dummy', 'dummy_token').token)
        self.assertEqual('kbc_at_dummy',
                         Endpoint(BASE_URL, 'dummy', BearerToken('kbc_at_dummy', 1234)).token)

    def test_missing_token(self):
        with self.assertRaisesRegex(ValueError, 'Token is required.'):
            Endpoint(BASE_URL, 'dummy', '')

    @responses.activate
    def test_caller_headers_are_kept_and_not_mutated(self):
        _mock_get()
        caller_headers = {'x-foo': 'bar'}
        Endpoint(BASE_URL, 'dummy', 'dummy_token')._get(URL, headers=caller_headers)

        headers = responses.calls[0].request.headers
        self.assertEqual('bar', headers['x-foo'])
        self.assertEqual('dummy_token', headers['X-StorageApi-Token'])
        self.assertEqual({'x-foo': 'bar'}, caller_headers)


if __name__ == '__main__':
    unittest.main()
