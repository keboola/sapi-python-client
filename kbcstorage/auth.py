from typing import Dict, Union

PROGRAMMATIC_PREFIXES = ('kbc_at_', 'kbc_pat_')


class StorageApiToken:
    """
    Legacy Storage API token, sent as ``X-StorageApi-Token``.
    """
    def __init__(self, token: str) -> None:
        if not token:
            raise ValueError("Token is required.")
        # never echo the token itself, the message can end up in logs
        if token.startswith(PROGRAMMATIC_PREFIXES):
            raise ValueError(
                "A token with a 'kbc_at_'/'kbc_pat_' prefix is a programmatic "
                "token and cannot be sent as X-StorageApi-Token. Use "
                "kbcstorage.auth.BearerToken with the project id instead.")
        self.token = token

    def headers(self) -> Dict[str, str]:
        return {'X-StorageApi-Token': self.token}


class BearerToken:
    """
    Programmatic session or personal access token (``kbc_at_*``, ``kbc_pat_*``).
    """
    def __init__(self, token: str, project_id: Union[str, int]) -> None:
        if not token:
            raise ValueError("Token is required.")
        if not project_id:
            raise ValueError("Project ID is required for a bearer token.")
        self.token = token
        # requests rejects non-string header values, and callers naturally pass
        # the numeric id straight from a token verify payload
        self.project_id = str(project_id)

    def headers(self) -> Dict[str, str]:
        return {'Authorization': 'Bearer {}'.format(self.token),
                'X-KBC-ProjectId': self.project_id}


Auth = Union[StorageApiToken, BearerToken]


def coerce(token: Union[str, Auth]) -> Auth:
    """
    Normalise the ``token`` argument of an endpoint into a strategy object.

    Args:
        token: A strategy object, or a bare token string. Endpoints build other
            endpoints and pass their own strategy on, so this runs once per
            endpoint and must stay free of side effects.

    Returns:
        The strategy object to authenticate with.
    """
    if isinstance(token, (StorageApiToken, BearerToken)):
        return token
    return StorageApiToken(token)
