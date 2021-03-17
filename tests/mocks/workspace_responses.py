list_response = [
    {
        "id": 234,
        "name": "boring_wozniak",
        "component": "wr-db",
        "configurationId": "aws-1",
        "created": "2016-05-17T11:11:20+0200",
        "connection": {
            "backend": "snowflake",
            "host": "keboola.snowflakecomputing.com",
            "database": "keboola_123",
            "schema": "boring_wozniak",
            "warehouse": "SAPI_PROD",
            "user": "xzy"
        },
        "creatorToken": {
          "id": 234,
          "description": "martin@keboola.com"
        },
        "creatorUser": {
            "id": 234,
            "name": "Martin"
        }
    }
]

detail_response = {
    "id": 1,
    "name": "boring_wozniak",
    "type": "table",
    "component": "wr-db",
    "configurationId": "aws-1",
    "created": "2016-05-17T11:11:20+0200",
    "connection": {
        "backend": "snowflake",
        "host": "keboola.snowflakecomputing.com",
        "database": "keboola_123",
        "schema": "boring_wozniak",
        "warehouse": "SAPI_PROD",
        "user": "xzy"
    },
    "creatorToken": {
        "id": 234,
        "description": "martin@keboola.com"
    },
    "creatorUser": {
        "id": 234,
        "name": "Martin"
    }
}

create_response = {
    "id": 234,
    "name": "boring_wozniak",
    "type": "table",
    "component": "wr-db",
    "configurationId": "aws-1",
    "created": "2016-05-17T11:11:20+0200",
    "connection": {
        "backend": "snowflake",
        "host": "keboola.snowflakecomputing.com",
        "database": "keboola_123",
        "schema": "boring_wozniak",
        "warehouse": "SAPI_PROD",
        "user": "xzy",
        "password": "abc"
    },
    "creatorToken": {
        "id": 234,
        "description": "martin@keboola.com"
    },
    "creatorUser": {
        "id": 234,
        "name": "Martin"
    }
}

load_tables_response = {
    "id": 22077337,
    "status": "waiting",
    "url": "https://connection.keboola.com/v2/storage/jobs/22077337",
    "tableId": None,
    "operationName": "workspaceLoad",
    "operationParams": {
        "workspaceId": "78423",
        "preserve": False,
        "input": [
            {
                "source": "in.c-application-testing.cashier-data",
                "destination": "my-table"
            }
        ],
        "queue": "main_fast"
    },
    "createdTime": "2017-02-13T16:41:18+0100",
    "startTime": None,
    "endTime": None,
    "runId": None,
    "results": None,
    "creatorToken": {
        "id": "27978",
        "description": "ondrej.popelka@keboola.com"
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

reset_password_response = {
    "password": "top_secret_password",
}
