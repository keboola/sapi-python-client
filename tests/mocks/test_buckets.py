import unittest
import responses
from kbcstorage.auth import BearerToken
from kbcstorage.buckets import Buckets
from .bucket_responses import list_response, detail_response, create_response


class TestBucketsWithMocks(unittest.TestCase):
    def setUp(self):
        token = 'dummy_token'
        base_url = 'https://connection.keboola.com/'
        self.buckets = Buckets(base_url, token)

    @responses.activate
    def test_list_tables_sends_one_auth_scheme_only(self):
        """
        list_tables passes its own headers to _get, so it is the one method that
        could ship both schemes at once.
        """
        url = 'https://connection.keboola.com/v2/storage/buckets/in.c-ga/tables'
        for token, expected, unexpected in (
            ('dummy_token', 'X-StorageApi-Token', 'Authorization'),
            (BearerToken('kbc_at_dummy', 1234), 'Authorization', 'X-StorageApi-Token'),
        ):
            with self.subTest(token=token):
                responses.reset()
                responses.add(responses.Response(method='GET', url=url, json=[]))
                Buckets('https://connection.keboola.com/', token).list_tables('in.c-ga')

                headers = responses.calls[0].request.headers
                self.assertIn(expected, headers)
                self.assertNotIn(unexpected, headers)

    @responses.activate
    def test_list(self):
        """
        Buckets mocks list correctly.
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/buckets',
                json=list_response
            )
        )
        buckets_list = self.buckets.list()
        assert isinstance(buckets_list, list)

    @responses.activate
    def test_detail_by_id(self):
        """
        Buckets mocks detail by integer id correctly.
        """
        responses.add(
            responses.Response(
                method='GET',
                url=('https://connection.keboola.com/v2/storage/buckets/'
                     'in.c-ga'),
                json=detail_response
            )
        )
        bucket_id = 'in.c-ga'
        bucket_detail = self.buckets.detail(bucket_id)
        assert bucket_detail['id'] == 'in.c-ga'

    @responses.activate
    def test_delete(self):
        """
        Buckets mock deletes bucket by id.
        """
        responses.add(
            responses.Response(
                method='DELETE',
                url='https://connection.keboola.com/v2/storage/buckets/1?force=False&async=False',
                json={}
            )
        )
        bucket_id = '1'
        deleted_detail = self.buckets.delete(bucket_id, asynchronous=False)
        assert deleted_detail is None

    @responses.activate
    def test_create(self):
        """
        Buckets mock creates new bucket.
        """
        responses.add(
            responses.Response(
                method='POST',
                url='https://connection.keboola.com/v2/storage/buckets',
                json=create_response
            )
        )
        name = 'my-new-bucket'
        description = 'Some Description'
        backend = 'snowflake'
        created_detail = self.buckets.create(name=name,
                                             description=description,
                                             backend=backend)
        assert created_detail['id'] == 'in.c-{}'.format(name)
