"""
Asses basic functionality of the Workspace endpoint.
"""
import unittest
from urllib.parse import parse_qs

import responses
from requests import HTTPError

from kbcstorage.workspaces import Workspaces

from .token_responses import verify_token_response
from .workspace_responses import (list_response, detail_response,
                                  load_tables_response, create_response,
                                  keypair_create_response,
                                  reset_password_response)


class TestWorkspacesEndpointWithMocks(unittest.TestCase):
    """
    Test the methods of a Workspaces endpoint instance with mock HTTP responses
    """
    def setUp(self):
        token = 'dummy_token'
        base_url = 'https://connection.keboola.com/'
        self.ws = Workspaces(base_url, token)

    @responses.activate
    def test_list(self):
        """
        Workspace mocks list correctly
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/workspaces',
                json=list_response
            )
        )
        workspace_list = self.ws.list()
        assert isinstance(workspace_list, list)

    @responses.activate
    def test_detail_by_integer_id(self):
        """
        Workspace Endpoint can mock detail by integer id
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/workspaces/1',
                json=detail_response
            )
        )
        workspace_id = 1
        workspace_detail = self.ws.detail(workspace_id)
        assert workspace_detail['id'] == 1

    @responses.activate
    def test_detail_by_str_id(self):
        """
        Workspace Endpoint can get mocked detail by string id
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/workspaces/1',
                json=detail_response
            )
        )
        workspace_id = '1'
        workspace_detail = self.ws.detail(workspace_id)
        assert workspace_detail['id'] == 1

    @responses.activate
    def test_detail_inexsitent_workspace(self):
        """
        Workspace Endpoint raises HTTPError when mocking inexistent workspace
        detail
        """
        msg = ('404 Client Error: Not Found for url: '
               'https://connection.keboola.com/v2/storage/workspaces/1')
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/workspaces/1',
                body=HTTPError(msg)
            )
        )
        workspace_id = '1'
        with self.assertRaises(HTTPError) as error_context:
            self.ws.detail(workspace_id)
        assert error_context.exception.args[0] == msg

    @responses.activate
    def test_create(self):
        """
        Workspace endpoint mock creates new workspace. With no backend given,
        the project default backend (snowflake) is resolved from the token and
        a key-pair workspace is created instead of a password one.
        """
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/tokens/verify',
                json=verify_token_response
            )
        )
        responses.add(
            responses.Response(
                method='POST',
                url='https://connection.keboola.com/v2/storage/workspaces',
                json=keypair_create_response
            )
        )
        created_detail = self.ws.create()
        request_body = parse_qs(responses.calls[1].request.body)
        assert request_body['backend'] == ['snowflake']
        assert request_body['loginType'] == ['snowflake-service-keypair']
        assert 'BEGIN PUBLIC KEY' in request_body['publicKey'][0]
        assert 'BEGIN PRIVATE KEY' in created_detail['connection']['privateKey']

    @responses.activate
    def test_create_snowflake_defaults_to_keypair(self):
        """
        With an explicit snowflake backend no token verify is needed and the
        login type defaults to snowflake-service-keypair with a locally
        generated key pair.
        """
        responses.add(
            responses.Response(
                method='POST',
                url='https://connection.keboola.com/v2/storage/workspaces',
                json=keypair_create_response
            )
        )
        created_detail = self.ws.create(backend='snowflake')
        assert len(responses.calls) == 1
        request_body = parse_qs(responses.calls[0].request.body)
        assert request_body['loginType'] == ['snowflake-service-keypair']
        assert 'BEGIN PUBLIC KEY' in request_body['publicKey'][0]
        assert 'BEGIN PRIVATE KEY' in created_detail['connection']['privateKey']

    @responses.activate
    def test_create_snowflake_with_own_public_key(self):
        """
        A caller-supplied public key is passed through and no private key is
        injected into the response.
        """
        responses.add(
            responses.Response(
                method='POST',
                url='https://connection.keboola.com/v2/storage/workspaces',
                json=keypair_create_response
            )
        )
        created_detail = self.ws.create(backend='snowflake', public_key='my-public-key')
        request_body = parse_qs(responses.calls[0].request.body)
        assert request_body['loginType'] == ['snowflake-service-keypair']
        assert request_body['publicKey'] == ['my-public-key']
        assert 'privateKey' not in created_detail['connection']

    @responses.activate
    def test_create_snowflake_explicit_legacy_login_type(self):
        """
        An explicitly requested password login type is passed through
        unchanged and no key pair is generated.
        """
        responses.add(
            responses.Response(
                method='POST',
                url='https://connection.keboola.com/v2/storage/workspaces',
                json=create_response
            )
        )
        created_detail = self.ws.create(backend='snowflake', login_type='snowflake-legacy-service')
        request_body = parse_qs(responses.calls[0].request.body)
        assert request_body['loginType'] == ['snowflake-legacy-service']
        assert 'publicKey' not in request_body
        assert created_detail['connection']['password'] == 'abc'

    @responses.activate
    def test_create_non_snowflake_backend_unchanged(self):
        """
        Non-snowflake backends keep the original behavior - no login type or
        public key is sent.
        """
        responses.add(
            responses.Response(
                method='POST',
                url='https://connection.keboola.com/v2/storage/workspaces',
                json=create_response
            )
        )
        self.ws.create(backend='bigquery')
        request_body = parse_qs(responses.calls[0].request.body)
        assert request_body['backend'] == ['bigquery']
        assert 'loginType' not in request_body
        assert 'publicKey' not in request_body

    @responses.activate
    def test_delete(self):
        """
        Workspace endpoint mock deletes workspace by id
        """
        responses.add(
            responses.Response(
                method='DELETE',
                url='https://connection.keboola.com/v2/storage/workspaces/1',
                json=create_response
            )
        )
        workspace_id = '1'
        deleted_detail = self.ws.delete(workspace_id)
        assert deleted_detail is None

    @responses.activate
    def test_delete_inexistent_workspace_raises_404(self):
        """
        Workspace endpoint raises 404 when mock deleting inexsitent workspace
        """
        msg = ('404 Client Error: Not Found for url: '
               'https://connection.keboola.com/v2/storage/workspaces/1')
        responses.add(
            responses.Response(
                method='DELETE',
                url='https://connection.keboola.com/v2/storage/workspaces/1',
                body=HTTPError(msg)
            )
        )
        workspace_id = '1'
        with self.assertRaises(HTTPError) as error_context:
            self.ws.delete(workspace_id)
        assert error_context.exception.args[0] == msg

    @responses.activate
    def test_load_tables_to_workspace(self):
        """
        Workspace endpoint mock loads table
        """
        responses.add(
            responses.Response(
                method='POST',
                url=('https://connection.keboola.com/v2/storage/workspaces/'
                     '78432/load'),
                json=load_tables_response
            )
        )
        workspace_id = '78432'
        mapping = {"in.c-application-testing.cashier-data": "my-table"}
        loaded_detail = self.ws.load_tables(workspace_id, mapping)
        assert loaded_detail['id'] == 22077337

    @responses.activate
    def test_load_inexistent_tables_to_workspace(self):
        """
        Workspace endpoint raises HTTPError when mock loading inexistent table
        """
        msg = ('404 Client Error: Not Found for url: '
               'https://connection.keboola.com/v2/storage/workspaces/78432/'
               'load')
        responses.add(
            responses.Response(
                method='POST',
                url=('https://connection.keboola.com/v2/storage/workspaces/'
                     '78432/load'),
                body=HTTPError(msg)
            )
        )
        workspace_id = '78432'
        mapping = {"in.c-table.does_not_exist": "my-table"}
        with self.assertRaises(HTTPError) as error_context:
            self.ws.load_tables(workspace_id, mapping)
        assert error_context.exception.args[0] == msg

    @responses.activate
    def test_reset_workspace_password(self):
        """
        Workspace endpoint mock resets password for workspace
        """
        responses.add(
            responses.Response(
                method='POST',
                url=('https://connection.keboola.com/v2/storage/workspaces/'
                     '1/password'),
                json=reset_password_response
            )
        )
        workspace_id = '1'
        reset_detail = self.ws.reset_password(workspace_id)
        assert reset_detail['password'] == 'top_secret_password'

    @responses.activate
    def test_reset_password_for_inexistent_workspace(self):
        """
        Workspace endpoint raises HTTPError when mock resetting password for
        inexistent workspace
        """
        msg = ('404 Client Error: Not Found for url: '
               'https://connection.keboola.com/v2/storage/workspaces/1/'
               'password')
        responses.add(
            responses.Response(
                method='POST',
                url=('https://connection.keboola.com/v2/storage/workspaces/'
                     '1/password'),
                body=HTTPError(msg)
            )
        )
        workspace_id = '1'
        with self.assertRaises(HTTPError) as error_context:
            self.ws.reset_password(workspace_id)
        assert error_context.exception.args[0] == msg

    @responses.activate
    def test_load_files_to_invalid_workspace(self):
        """
        Raises exception when mock loading_files to invalid workspace
        """
        msg = ('Loading files to workspace is only available for ABS workspaces')
        responses.add(
            responses.Response(
                method='GET',
                url='https://connection.keboola.com/v2/storage/workspaces/1',
                json=detail_response
            )
        )
        workspace_id = '1'
        try:
            self.ws.load_files(workspace_id, {'tags': ['sapi-client-python-tests'], 'destination': 'data/in/files'})
        except Exception as ex:
            assert str(ex) == msg
