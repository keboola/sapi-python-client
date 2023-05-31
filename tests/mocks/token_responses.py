verify_token_response = {
    "id": "373115",
    "created": "2021-06-23T09:33:36+0200",
    "refreshed": "2021-06-23T09:33:36+0200",
    "description": "devel@keboola.com",
    "uri": "https://connection.keboola.com/v2/storage/tokens/373115",
    "isMasterToken": True,
    "canManageBuckets": True,
    "canManageTokens": True,
    "canReadAllFileUploads": True,
    "canPurgeTrash": True,
    "expires": None,
    "isExpired": False,
    "isDisabled": False,
    "dailyCapacity": 0,
    "token": "8625-373115-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "bucketPermissions": {
        "out.c-test2": "manage",
        "in.c-test3": "manage",
        "out.c-test": "manage",
        "out.c-some-other": "manage"
    },
    "owner": {
        "id": 8625,
        "name": "Test Project",
        "type": "production",
        "region": "us-east-1",
        "created": "2021-06-23T09:33:32+0200",
        "expires": None,
        "features": [
            "syrup-jobs-limit-10",
            "oauth-v3",
            "queuev2",
            "storage-types",
            "allow-ai",
            "alternat",
        ],
        "dataSizeBytes": 31474176,
        "rowsCount": 797,
        "hasMysql": False,
        "hasSynapse": False,
        "hasRedshift": True,
        "hasSnowflake": True,
        "hasExasol": False,
        "hasTeradata": False,
        "hasBigquery": False,
        "defaultBackend": "snowflake",
        "hasTryModeOn": "0",
        "limits": {
            "components.jobsParallelism": {
                "name": "components.jobsParallelism",
                "value": 10
            },
            "goodData.dataSizeBytes": {
                "name": "goodData.dataSizeBytes",
                "value": 1000000000
            },
            "goodData.demoTokenEnabled": {
                "name": "goodData.demoTokenEnabled",
                "value": 1
            },
            "goodData.prodTokenEnabled": {
                "name": "goodData.prodTokenEnabled",
                "value": 0
            },
            "goodData.usersCount": {
                "name": "goodData.usersCount",
                "value": 30
            },
            "kbc.adminsCount": {
                "name": "kbc.adminsCount",
                "value": 10
            },
            "kbc.extractorsCount": {
                "name": "kbc.extractorsCount",
                "value": 0
            },
            "kbc.monthlyProjectPowerLimit": {
                "name": "kbc.monthlyProjectPowerLimit",
                "value": 50
            },
            "kbc.writersCount": {
                "name": "kbc.writersCount",
                "value": 0
            },
            "orchestrations.count": {
                "name": "orchestrations.count",
                "value": 10
            },
            "storage.dataSizeBytes": {
                "name": "storage.dataSizeBytes",
                "value": 50000000000
            },
            "storage.jobsParallelism": {
                "name": "storage.jobsParallelism",
                "value": 10
            }
        },
        "metrics": {
            "kbc.adminsCount": {
                "name": "kbc.adminsCount",
                "value": 1
            },
            "orchestrations.count": {
                "name": "orchestrations.count",
                "value": 0
            },
            "storage.dataSizeBytes": {
                "name": "storage.dataSizeBytes",
                "value": 31474176
            },
            "storage.rowsCount": {
                "name": "storage.rowsCount",
                "value": 797
            }
        },
        "isDisabled": False,
        "billedMonthlyPrice": None,
        "dataRetentionTimeInDays": 7,
        "fileStorageProvider": "aws",
        "redshift": {
            "connectionId": 365,
            "databaseName": "sapi_8625"
        }
    },
    "organization": {
        "id": "111111"
    },
    "admin": {
        "name": "Devel",
        "id": 59,
        "features": [
            "ui-devel-preview",
            "manage-try-mode",
            "validate-sql",
            "early-adopter-preview"
        ],
        "isOrganizationMember": True,
        "role": "admin"
    }
}
