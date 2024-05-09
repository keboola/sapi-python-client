list_response = [
    {
        "id": "3",
        "runWithTokenId": 123,
        "component": "orchestration",
        "configurationId": "config-100",
        "lastRun": "2017-02-13T16:42:00+0100",
        "coolDownPeriodMinutes": 20,
        "creatorToken": {
            "id": 1,
            "description": "dev@keboola.com"
        },
        "tables": [
            {
                "tableId": "in.c-test.watched-1"
            },
            {
                "tableId": "in.c-prod.watched-5"
            }
        ]
    }
]

detail_response = {
    "id": "3",
    "runWithTokenId": 123,
    "component": "orchestration",
    "configurationId": "config-100",
    "lastRun": "2017-02-13T16:42:00+0100",
    "coolDownPeriodMinutes": 20,
    "creatorToken": {
        "id": 1,
        "description": "dev@keboola.com"
    },
    "tables": [
        {
            "tableId": "in.c-test.watched-1"
        },
        {
            "tableId": "in.c-prod.watched-5"
        }
    ]
}

update_response = {
    "id": "3",
    "runWithTokenId": 123,
    "component": "orchestration",
    "configurationId": "config-100",
    "lastRun": "2017-02-13T16:42:00+0100",
    "coolDownPeriodMinutes": 20,
    "creatorToken": {
        "id": 1,
        "description": "dev@keboola.com"
    },
    "tables": [
        {
            "tableId": "in.c-test.watched-1"
        },
        {
            "tableId": "in.c-prod.watched-5"
        }
    ]
}

create_response = {
    "runWithTokenId": 123,
    "component": "orchestration",
    "configurationId": 123,
    "coolDownPeriodMinutes": 20,
    "tableIds": [
        "in.c-test.watched-1",
        "in.c-prod.watched-5"
    ]
}
