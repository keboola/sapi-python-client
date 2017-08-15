from setuptools import setup, find_packages
import kbcstorage

with open("README.md") as f:
    readme = f.read()

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
    long_description=readme,
    license="MIT"
)
