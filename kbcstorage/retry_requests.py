import time
import requests

MAX_RETRIES_DEFAULT = 11
BACKOFF_FACTOR = 1.0


def _get_backoff_time(retry_count):
    return BACKOFF_FACTOR * (2 ** retry_count)


class RetryRequests:
    def __init__(self, max_requests_retries=MAX_RETRIES_DEFAULT) -> None:
        self.max_retries = max_requests_retries

    def _retry_request(self, request_func, url, *args, **kwargs):
        response = request_func(url, *args, **kwargs)
        for retry_count in range(self.max_retries - 1):
            if response.status_code == 501 or response.status_code < 500:
                return response
            time.sleep(_get_backoff_time(retry_count))
            response = request_func(url, **kwargs)
        return response

    def get(self, url, *args, **kwargs):
        return self._retry_request(requests.get, url, *args, **kwargs)

    def post(self, url, *args, **kwargs):
        return self._retry_request(requests.post, url, *args, **kwargs)

    def put(self, url, *args, **kwargs):
        return self._retry_request(requests.put, url, *args, **kwargs)

    def delete(self, url, *args, **kwargs):
        return self._retry_request(requests.delete, url, *args, **kwargs)
