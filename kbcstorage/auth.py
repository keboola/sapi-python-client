import os
import sys
import warnings
from typing import Dict, Union

PROGRAMMATIC_PREFIXES = ('kbc_at_', 'kbc_pat_')

_PKG_DIR = os.path.dirname(os.path.abspath(__file__))


def _caller_stacklevel() -> int:
    """
    Frames to skip so a warning lands on the first caller outside this package.

    Endpoints build other endpoints, so a fixed stacklevel points at kbcstorage
    itself: the message names the wrong line, the same warning repeats once per
    endpoint, and a DeprecationWarning attributed to a library module is dropped
    by the default filter.
    """
    level, frame = 1, sys._getframe(1)
    while frame is not None and os.path.dirname(frame.f_code.co_filename) == _PKG_DIR:
        level += 1
        frame = frame.f_back
    return level


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
        token: A strategy object, or a bare token string for backward
            compatibility.

    Returns:
        The strategy object to authenticate with.
    """
    if isinstance(token, StorageApiToken):
        return token

    if isinstance(token, BearerToken):
        warnings.warn(
            "Authenticating with a bearer token: `.token` returns the "
            "programmatic token, not a Storage API token, and will be "
            "rejected if sent as X-StorageApi-Token. Pass `.auth` when "
            "building further endpoints.",
            UserWarning,
            stacklevel=_caller_stacklevel(),
        )
        return token
    auth = StorageApiToken(token)
    warnings.warn(
        "Passing the token as a string is deprecated, "
        "pass kbcstorage.auth.StorageApiToken instead.",
        DeprecationWarning,
        stacklevel=_caller_stacklevel(),
    )
    return auth
