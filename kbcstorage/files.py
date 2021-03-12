"""
Manages calls to the Storage API relating to files

Full documentation `here`.

.. _here:
    http://docs.keboola.apiary.io/#reference/files/
"""
import json
import os
import boto3
import requests

from azure.storage.blob import BlobServiceClient, ContentSettings
from kbcstorage.base import Endpoint


class Files(Endpoint):
    """
    Buckets Endpoint
    """
    def __init__(self, root_url, token):
        """
        Create a Files endpoint.

        Args:
            root_url (:obj:`str`): The base url for the API.
            token (:obj:`str`): A storage API key.
        """
        super().__init__(root_url, 'files', token)

    def detail(self, file_id, federation_token=False):
        """
        Retrieves information about a given file.

        Args:
            file_id (str): The id of the file.
            federation_token (bool): True to get AWS credentials
                for file download

        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}'.format(self.base_url, file_id)
        params = {}
        if federation_token:
            params['federationToken'] = 'true'
        return self._get(url, params=params)

    def upload_file(self, file_path, tags=None, is_public=False,
                    is_permanent=False, is_encrypted=True,
                    is_sliced=False, do_notify=False, compress=False):
        """
        Upload a file to storage

        Args:
            file_path (str): Local path to file to upload
            tags (list): Array of tags
            is_public (bool): File is public
            is_permanent (bool): File is permanent
            is_encrypted (bool): File is encrypted
            is_sliced (bool): File is sliced
            do_notify (bool): Notify members of project that file was uploaded

        Returns:
            file_id (str): Id of the created file

        Raises:
            requests.HTTPError: If the API request fails.
        """
        if not os.path.exists(file_path) or not os.path.isfile(file_path):
            raise ValueError("File " + file_path + " does not exist")
        if compress:
            import gzip
            import shutil
            with open(file_path, 'rb') as f_in, \
                    gzip.open(file_path + '.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            file_path = file_path + '.gz'
        file_name = os.path.basename(file_path)
        size = os.path.getsize(file_path)
        file_resource = self.prepare_upload(file_name, size, tags, is_public,
                                            is_permanent, is_encrypted,
                                            is_sliced, do_notify, True)
        if file_resource['provider'] == 'azure':
            self.__upload_to_azure(file_resource, file_path)
        elif file_resource['provider'] == 'aws':
            self.__upload_to_aws(file_resource, file_path, is_encrypted)

        return file_resource['id']

    def prepare_upload(self, name, size_bytes=None, tags=None, is_public=False,
                       is_permanent=False, is_encrypted=True,
                       is_sliced=False, do_notify=False,
                       federation_token=True):
        """
        Prepare a file resource for a new file

        Args:
            name (str): The new file name (only alphanumeric and underscores)
            size_bytes (int): Size of the file
            tags (list): Array of tags
            is_public (bool): File is public
            is_permanent (bool): File is permanent
            is_encrypted (bool): File is encrypted
            is_sliced (bool): File is sliced
            do_notify (bool): Notify members of project that file was uploaded
            federation_token (bool): Obtain AWS federation token

        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        url = '{}/{}'.format(self.base_url, 'prepare')
        body = {
            'isPublic': int(is_public),
            'isPermanent': int(is_permanent),
            'isEncrypted': int(is_encrypted),
            'isSliced': int(is_sliced),
            'notify': int(do_notify),
            'name': name
        }
        if tags is not None and isinstance(tags, list):
            body['tags[]'] = tags
        if size_bytes is not None:
            body['sizeBytes'] = size_bytes
        if federation_token is not None:
            body['federationToken'] = int(federation_token)
        return self._post(url, data=body)

    def delete(self, file_id):
        """
        Delete a bucket referenced by ``file_id``.

        Args:
            file_id (str): The id of the file to be deleted.
        """
        url = '{}/{}'.format(self.base_url, file_id)
        self._delete(url)

    def list(self, limit=100, offset=0, tags=None, q=None, run_id=None,
             since_id=None, max_id=None):
        """
        List files in project.

        Args:
            limit (int): Pagination size
            offset (int): Pagination start
            tags (list): List files with the tags
            q (str) Elastic query string
            run_id (str) Run Id
            since_id (str) List files with ID bigger than
            max_id (str) List files with ID smaller than

        Returns:
            response_body: The parsed json from the HTTP response.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        params = {
            'limit': int(limit),
            'offset': int(offset)
        }
        if tags is not None and isinstance(tags, list):
            params['tags[]'] = tags
        if q is not None:
            params['q'] = q
        if run_id is not None:
            params['sinceId'] = since_id
        if max_id is not None:
            params['maxId'] = max_id
        return self._get(self.base_url, params=params)

    def download(self, file_id, local_path):
        if not os.path.exists(local_path):
            os.mkdir(local_path)
        file_info = self.detail(file_id=file_id, federation_token=True)
        local_file = os.path.join(local_path, file_info['name'])
        if file_info['provider'] == 'azure':
            if file_info['isSliced']:
                self.__download_sliced_file_from_azure(file_info, local_file)
            else:
                self.__download_file_from_azure(file_info, local_file)
        elif file_info['provider'] == 'aws':
            s3 = boto3.resource(
                's3',
                aws_access_key_id=file_info['credentials']['AccessKeyId'],
                aws_secret_access_key=file_info['credentials']['SecretAccessKey'],
                aws_session_token=file_info['credentials']['SessionToken'],
                region_name=file_info['region']
            )
            if file_info['isSliced']:
                self.__download_sliced_file_from_aws(file_info, local_file, s3)
            else:
                self.__download_file_from_aws(file_info, local_file, s3)
        return local_file

    def __upload_to_azure(self, preparation_result, file_path):
        blob_client = self.__get_blob_client(
            preparation_result['absUploadParams']['absCredentials']['SASConnectionString'],
            preparation_result['absUploadParams']['container'],
            preparation_result['absUploadParams']['blobName']
        )
        with open(file_path, "rb") as blob_data:
            blob_client.upload_blob(
                blob_data,
                blob_type='BlockBlob',
                content_settings=ContentSettings(
                    content_disposition='attachment;filename="%s"' % (preparation_result['name'])
                )
            )

    def __upload_to_aws(self, prepare_result, file_path, is_encrypted):
        upload_params = prepare_result['uploadParams']
        key_id = upload_params['credentials']['AccessKeyId']
        key = upload_params['credentials']['SecretAccessKey']
        token = upload_params['credentials']['SessionToken']
        s3 = boto3.resource('s3', aws_access_key_id=key_id,
                            aws_secret_access_key=key,
                            aws_session_token=token,
                            region_name=prepare_result['region'])
        s3_object = s3.Object(bucket_name=upload_params['bucket'], key=upload_params['key'])
        disposition = 'attachment; filename={};'.format(prepare_result['name'])
        with open(file_path, mode='rb') as file:
            if is_encrypted:
                encryption = upload_params['x-amz-server-side-encryption']
                s3_object.put(ACL=upload_params['acl'], Body=file,
                              ContentDisposition=disposition, ServerSideEncryption=encryption)
            else:
                s3_object.put(ACL=upload_params['acl'], Body=file,
                              ContentDisposition=disposition)

    def __download_file_from_aws(self, file_info, destination, s3):
        bucket = s3.Bucket(file_info["s3Path"]["bucket"])
        bucket.download_file(file_info["s3Path"]["key"], destination)

    def __download_sliced_file_from_aws(self, file_info, destination, s3):
        manifest = requests.get(url=file_info['url']).json()
        file_names = []
        for entry in manifest["entries"]:
            full_path = entry["url"]
            file_name = full_path.rsplit("/", 1)[1]
            file_names.append(file_name)
            splitted_path = full_path.split("/")
            file_key = "/".join(splitted_path[3:])
            bucket = s3.Bucket(file_info['s3Path']['bucket'])
            bucket.download_file(file_key, file_name)
        self.__merge_split_files(file_names, destination)

    def __download_file_from_azure(self, file_info, destination):
        blob_client = self.__get_blob_client(
            file_info['absCredentials']['SASConnectionString'],
            file_info['absPath']['container'],
            file_info['absPath']['name']
        )
        with open(destination, "wb") as downloaded_blob:
            download_stream = blob_client.download_blob()
            downloaded_blob.write(download_stream.readall())

    def __download_sliced_file_from_azure(self, file_info, destination):
        blob_service_client = BlobServiceClient.from_connection_string(
            file_info['absCredentials']['SASConnectionString']
        )
        container_client = blob_service_client.get_container_client(
            container=file_info['absPath']['container']
        )
        manifest_stream = container_client.download_blob(
            file_info['absPath']['name'] + 'manifest'
        )
        manifest = json.loads(manifest_stream.readall())
        file_names = []
        for entry in manifest['entries']:
            blob_path = entry['url'].split('blob.core.windows.net/%s/' % (file_info['absPath']['container']))[1]
            full_path = entry["url"]
            file_name = full_path.rsplit("/", 1)[1]
            file_names.append(file_name)
            with open(file_name, "wb") as file_slice:
                file_slice.write(container_client.download_blob(blob_path).readall())
        self.__merge_split_files(file_names, destination)

    def __merge_split_files(self, file_names, destination):
        with open(destination, mode='wb') as out_file:
            for file_name in file_names:
                with open(file_name, mode='rb') as in_file:
                    for line in in_file:
                        out_file.write(line)
                os.remove(file_name)

    def __get_blob_client(self, connection_string, container, blob_name):
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        return blob_service_client.get_blob_client(container=container, blob=blob_name)
