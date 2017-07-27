from setuptools import setup, find_packages
import kbcstorage
setup(
    name='kbcstorage',
    version=kbcstorage.__version__,
    url='https://github.com/keboola/sapi-python-client',
    download_url='https://github.com/keboola/sapi-python-client',
    packages=find_packages(),
    install_requires=[
        'boto3',
        'requests'
    ]
)
