class StorageApiToken:
    """
    Legacy Storage API token, sent as ``X-StorageApi-Token``.
    """
    def __init__(self, token):
        if not token:
            raise ValueError("Token is required.")
        self.token = token

    def headers(self):
        return {'X-StorageApi-Token': self.token}


class BearerToken:
    """
    Programmatic session or personal access token (``kbc_at_*``, ``kbc_pat_*``).
    """
    def __init__(self, token, project_id):
        if not token:
            raise ValueError("Token is required.")
        if not project_id:
            raise ValueError("Project ID is required for a bearer token.")
        self.token = token
        # requests rejects non-string header values, and callers naturally pass
        # the numeric id straight from a token verify payload
        self.project_id = str(project_id)

    def headers(self):
        return {'Authorization': 'Bearer {}'.format(self.token),
                'X-KBC-ProjectId': self.project_id}


def coerce(token):
    """
    Normalise the ``token`` argument of an endpoint into a strategy object.

    Args:
        token: A strategy object, or a bare token string for backward
            compatibility.

    Returns:
        The strategy object to authenticate with.
    """
    if hasattr(token, 'headers'):
        return token
    return StorageApiToken(token)
