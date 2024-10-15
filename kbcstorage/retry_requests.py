import time
import requests

MAX_RETRIES = 11
BACKOFF_FACTOR = 1.0


def _get_backoff_time(retry_count):
    return BACKOFF_FACTOR * (2 ** retry_count)


def _retry_request(request_func, url, *args, **kwargs):
    response = request_func(url, *args, **kwargs)
    for retry_count in range(MAX_RETRIES - 1):
        if response.status_code == 501 or response.status_code < 500:
            return response
        time.sleep(_get_backoff_time(retry_count))
        response = request_func(url, **kwargs)
    return response


def get(url, *args, **kwargs):
    return _retry_request(requests.get, url, *args, **kwargs)


def post(url, *args, **kwargs):
    return _retry_request(requests.post, url, *args, **kwargs)


def put(url, *args, **kwargs):
    return _retry_request(requests.put, url, *args, **kwargs)


def delete(url, *args, **kwargs):
    return _retry_request(requests.delete, url, *args, **kwargs)
