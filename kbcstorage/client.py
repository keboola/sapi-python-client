import boto3
from botocore.exceptions import ClientError
import os
import requests
import string
import time


class HttpHelper:
    server = "https://connection.keboola.com/v2/"
    user_agent = "Keboola StorageApi Python Client/v2"

    def __init__(self, token):
        self.token = token

    def tokenheader(self):
        return {
            'User-Agent': self.user_agent,
            'X-StorageApi-Token': self.token
        }

    def getRequest(self, url, params = None):
        if params == None:
            params = []
        resp = self.getAbsUrlRequest(self.server + url, params)
        resp.raise_for_status()
        return resp.json()

    def postRequest(self, url, params):
        resp = requests.post(self.server + url, headers=self.tokenheader(), data=params)
        resp.raise_for_status()
        return resp.json()

    def getAbsUrlRequest(self, url, params):
        return requests.get(url, headers=self.tokenheader(), params=params)


class Client:
    def __init__(self, token):
        self.token = token
        self.http = HttpHelper(token)

    def list_buckets(self):
        return self.http.getRequest('storage/buckets')

    def list_bucket_tables(self, bucket):
        return self.http.getRequest('storage/buckets/' + bucket + '/tables')

    def files_prepare(self, name, sizeBytes):
        payload = {'name': name, 'sizeBytes': sizeBytes, 'notify': False}
        return self.http.postRequest('storage/files/prepare', payload)

    def files_upload(self, path):
        fileObject = open(path, 'rb')
        fileResource = self.files_prepare(os.path.basename(path), os.path.getsize(path))
        uploadParams = fileResource['uploadParams']
        params = {
            'key': uploadParams['key'],
            'acl': uploadParams['acl'],
            'signature': uploadParams['signature'],
            'policy': uploadParams['policy'],
            'AWSAccessKeyId': uploadParams['AWSAccessKeyId']
        }
        files = {'file': fileObject}
        requests.post(uploadParams['url'], data=params, files=files)
        return fileResource

    def bucket_exists(self, bucketId):
        resp = self.http.getRequest('storage/buckets/' + bucketId)
        if resp.status_code == 404:
            return False
        else:
            return True

    def table_exists(self, tableId):
        try:
            resp = self.http.getRequest('storage/tables/' + tableId)
            return True
        except requests.exceptions.HTTPError as e:
            return False

    def get_table(self, tableId):
        return self.http.getRequest('storage/tables/' + tableId)

    def get_bucket(self, bucketId):
        return self.http.getRequest('storage/buckets/' + bucketId)

    def load_table_async(self, tableId, options = None):
        opts = self.prepare_options(options)
        if "federationToken" not in opts:
            opts["federationToken"] = 1
        return self.http.postRequest("storage/tables/" + tableId + "/export-async", opts)

    def get_job_status(self, url):
        resp = self.http.getAbsUrlRequest(url, [])
        return resp.json()

    def get_file_info(self, fileId, federationToken = "1"):
        return self.http.getRequest("storage/files/" + str(fileId), {"federationToken": federationToken})

    def prepare_options(self, options = None):
        if options is None:
            options = {}
        # which parameters are allowed
        params = ["limit", "changedSince", "changedUntil", "whereColumn", "whereValues"]
        opts = {}
        for val in params:
            if val in options:
                opts[val] = options[val]

        if "columns" in options:
            opts["columns"] = string.split(options["columns"], ",")

        if "whereValues" in options:
            for val in options["whereValues"]:
                opts["whereValies[" + val + "]"] = options["whereValues"][val]
                #opts[[paste0("whereValues[", i - 1, "]")]] < - options[["whereValues"]][i]

        return opts

    def get_table_data(self, tableId, localFile, options = None):
        resp = self.load_table_async(tableId, options)
        retries = 1
        while True:
            job = self.get_job_status(resp["url"])
            if job["status"] == "success":
                break
            time.sleep(2 ^ retries)
            retries = retries + 1
            if job["status"] != "waiting" and job["status"] != "processing":
                raise Exception("Job status: " + job["status"] + " - " + job["error"]["message"] +  " {" + job["error"]["exceptionId"] + ")")

        table = self.get_table(tableId)
        fileInfo = self.get_file_info(job["results"]["file"]["id"])
        s3 = boto3.resource(
            's3',
            aws_access_key_id=fileInfo["credentials"]["AccessKeyId"],
            aws_secret_access_key=fileInfo["credentials"]["SecretAccessKey"],
            aws_session_token=fileInfo["credentials"]["SessionToken"]
        )

        if fileInfo["isSliced"]:
            manifest = self.http.getAbsUrlRequest(fileInfo["url"], []).json()
            fileNames = []
            for entry in manifest["entries"]:
                fullPath = entry["url"]
                fileName = fullPath.rsplit("/", 1)[1]
                fileNames.append(fileName)
                splittedPath = string.split(fullPath, "/")
                fileKey = "/".join(splittedPath[3:])
                bucket = s3.Bucket(fileInfo["s3Path"]["bucket"])
                try:
                    bucket.download_file(fileKey, fileName)
                except ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("Could not find the object in s3")
                    else:
                        raise

            # merge the downloaded files
            with open(localFile, 'w') as outfile:
                for fileName in fileNames:
                    with open(fileName) as infile:
                        for line in infile:
                            outfile.write(line)
                    os.remove(fileName)

        else:
            # single file is friendlier
            bucket = s3.Bucket(fileInfo["s3Path"]["bucket"])
            bucket.download_file(fileInfo["s3Path"]["key"], localFile)


    def save_table(self, tableName, bucket, localFilePath, options = None):

        if options is None:
            options = {}

        tableId = bucket + "." + tableName
        postUrl = "storage/buckets/" + bucket + "/tables-async"
        if self.table_exists(tableId):
            postUrl = "storage/tables/" + tableId + "/import-async"

        resource = self.files_upload(localFilePath)
        opts = {
            "bucketId": bucket,
            "name": tableName,
            "dataFileId": resource["id"]
        }
        opts['delimeter'] = options["delimeter"] if "delimeter" in options else ","
        opts['enclosure'] = options["enclosure"] if "enclosure" in options else '"'
        opts['escapedBy'] = options["escapedBy"] if "escapedBy" in options else None
        opts['primaryKey'] = options["primaryKey"] if "primaryKey" in options else None
        opts['incremental'] = options["incremental"] if "incremental" in options else None

        jobres = self.http.postRequest(postUrl, opts)

        retries = 1
        while True:
            job = self.get_job_status(jobres["url"])
            if job["status"] == "success":
                break
            time.sleep(2 ^ retries)
            retries = retries + 1
            if job["status"] != "waiting" and job["status"] != "processing":
                raise Exception(
                    "Job status: " + job["status"] + " - " + job["error"]["message"]
                    + " {" + job["error"]["exceptionId"] + ")"
                )
        return True
