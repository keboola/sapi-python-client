list_response = [
    {
        "uri": "https://connection.keboola.com/v2/storage/buckets/in.c-main",
        "id": "in.c-main",
        "name": "c-main",
        "stage": "in",
        "description": "Main user storage",
        "tables": ("https://connection.keboola.com/v2/storage/buckets/"
                   "in.c-main/tables"),
        "backend": "snowflake"
    },
    {
        "uri": ("https://connection.keboola.com/v2/storage/buckets/"
                "in.c-organizationData"),
        "id": "in.c-organizationData",
        "name": "c-organizationData",
        "stage": "in",
        "description": "Source bucket description",
        "tables": ("https://connection.keboola.com/v2/storage/buckets/"
                   "in.c-organizationData/tables"),
        "backend": "snowflake",
        "isReadonly": True,
        "sourceBucket": {
            "id": "in.c-main",
            "name": "c-main",
            "description": "Organization shared data",
            "project": {
                "id": 123,
                "name": "Project name"
            }
        }
    }
]

detail_response = {
    "uri": "https://connection.keboola.com/v2/storage/buckets/in.c-ga",
    "id": "in.c-ga",
    "name": "c-ga",
    "stage": "in",
    "description": "Google Analytics",
    "tables": "https://connection.keboola.com/v2/storage/buckets/in.c-ga/tables",
    "backend": "mysql"
}

create_response = {
    "uri": ("https://connection.keboola.com/v2/storage/buckets/"
            "in.c-my-new-bucket"),
    "id": "in.c-my-new-bucket",
    "name": "c-my-new-bucket",
    "stage": "in",
    "description": "Some Description",
    "tables": ("https://connection.keboola.com/v2/storage/buckets/"
               "in.c-my-new-bucket/tables"),
    "created": "2017-02-13T12:01:05+0100",
    "lastChangeDate": None,
    "isReadOnly": False,
    "dataSizeBytes": 0,
    "rowsCount": 0,
    "isMaintenance": False,
    "backend": "snowflake",
    "sharing": None,
    "attributes": []
}

create_definition_response = {
    "id": 145390544,
    "status": "success",
    "url": "https:\/\/connection.eu-central-1.keboola.com\/v2\/storage\/jobs\/145390544",
    "tableId": None,
    "operationName": "tableDefinitionCreate",
    "operationParams": {
        "name": "my-new-table",
        "index": {
            "type": "CLUSTERED INDEX",
            "indexColumnsNames": [
                "id"
            ]
        },
        "queue": "main",
        "columns": [
            {
                "name": "id",
                "definition": {
                    "type": "INT",
                    "Noneable": True
                }
            },
            {
                "name": "name",
                "definition": {
                    "type": "NVARCHAR",
                    "Noneable": True
                }
            }
        ],
        "distribution": {
            "type": "HASH",
            "distributionColumnsNames": [
                "id"
            ]
        },
        "primaryKeysNames": [
            "id"
        ]
    },
    "createdTime": "2021-12-28T16:17:40+0100",
    "startTime": "2021-12-28T16:17:41+0100",
    "endTime": "2021-12-28T16:17:42+0100",
    "runId": "364533544",
    "results": {
        "uri": "https:\/\/connection.eu-central-1.keboola.com\/v2\/storage\/tables\/in.c-de-test.my-new-table",
        "id": "in.c-de-test.my-new-table",
        "name": "my-new-table",
        "displayName": "my-new-table",
        "transactional": False,
        "primaryKey": [
            "id"
        ],
        "indexType": "CLUSTERED INDEX",
        "indexKey": [
            "id"
        ],
        "distributionType": "HASH",
        "distributionKey": [
            "id"
        ],
        "syntheticPrimaryKeyEnabled": False,
        "indexedColumns": [
            "id"
        ],
        "created": "2021-12-28T16:17:41+0100",
        "lastImportDate": None,
        "lastChangeDate": None,
        "rowsCount": None,
        "dataSizeBytes": None,
        "isAlias": False,
        "isAliasable": True,
        "columns": [
            "id",
            "name"
        ]
    },
    "creatorToken": {
        "id": "164290",
        "description": "SAPI CLIENT TESTS"
    },
    "metrics": {
        "inCompressed": False,
        "inBytes": 0,
        "inBytesUncompressed": 0,
        "outCompressed": False,
        "outBytes": 0,
        "outBytesUncompressed": 0
    }
}
