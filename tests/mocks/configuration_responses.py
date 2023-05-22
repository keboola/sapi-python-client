list_response = [
    [
        {
            "id": "keboola.runner-config-test",
            "type": "application",
            "name": "runner-config-test",
            "description": "",
            "longDescription": None,
            "version": 32,
            "complexity": None,
            "categories": [],
            "hasUI": False,
            "hasRun": False,
            "ico32": None,
            "ico64": None,
            "ico128": None,
            "data": {
                "definition": {
                    "type": "aws-ecr",
                    "uri": "147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2"
                           "/keboola.runner-config-test",
                    "tag": "1.3.0",
                    "digest": "sha256:14a927e1eeef8e6c88de9715b69913d2c3beade3dbc9e2473041b7d9693183bf"
                },
                "vendor": {
                    "contact": [
                        "Keboola :(){:|:&};: s.r.o.",
                        "Dělnická 191/27\nHolešovice\n170 00 Praha 7",
                        "support@keboola.com"
                    ]
                },
                "configuration_format": "json",
                "network": "bridge",
                "forward_token": False,
                "forward_token_details": True,
                "default_bucket": False,
                "staging_storage": {
                    "input": "local",
                    "output": "local"
                },
                "synchronous_actions": [
                    "dumpConfig",
                    "dumpEnv",
                    "timeout",
                    "emptyJsonArray",
                    "emptyJsonObject",
                    "invalidJson",
                    "noResponse",
                    "userError",
                    "applicationError",
                    "printLogs"
                ]
            },
            "flags": [
                "excludeFromNewList"
            ],
            "configurationSchema": {},
            "configurationRowSchema": {},
            "emptyConfiguration": {},
            "emptyConfigurationRow": {},
            "uiOptions": {},
            "configurationDescription": None,
            "features": [
                "mlflow-artifacts-access"
            ],
            "expiredOn": None,
            "uri": "https://syrup.keboola.com/docker/keboola.runner-config-test",
            "configurations": [
                {
                    "id": "978755777",
                    "name": "test_create_configuration",
                    "description": "",
                    "created": "2023-05-22T00:26:41+0200",
                    "creatorToken": {
                        "id": 585402,
                        "description": "Dev"
                    },
                    "version": 1,
                    "changeDescription": "Configuration created",
                    "isDisabled": False,
                    "isDeleted": False,
                    "currentVersion": {
                        "created": "2023-05-22T00:26:41+0200",
                        "creatorToken": {
                            "id": 585402,
                            "description": "Dev"
                        },
                        "changeDescription": "Configuration created"
                    }
                }
            ]
        }
    ]
]

detail_response = {
    "id": "978755777",
    "name": "test_create_configuration",
    "description": "",
    "created": "2023-05-22T00:26:41+0200",
    "creatorToken": {
        "id": 585402,
        "description": "Dev"
    },
    "version": 1,
    "changeDescription": "Configuration created",
    "isDisabled": False,
    "isDeleted": False,
    "configuration": {},
    "rowsSortOrder": [],
    "rows": [],
    "state": {},
    "currentVersion": {
        "created": "2023-05-22T00:26:41+0200",
        "creatorToken": {
            "id": 585402,
            "description": "Dev"
        },
        "changeDescription": "Configuration created"
    }
}

create_response = {
    "id": "some-configuration-id",
    "name": "some-configuration",
    "description": "my-description",
    "created": "2023-05-22T07:46:58+0200",
    "creatorToken": {
        "id": 585402,
        "description": "Dev"
    },
    "version": 1,
    "changeDescription": "some-change-description",
    "isDisabled": False,
    "isDeleted": False,
    "configuration": {
        "parameters": {
            "foo": "bar"
        }
    },
    "state": {},
    "currentVersion": {
        "created": "2023-05-22T07:46:58+0200",
        "creatorToken": {
            "id": 585402,
            "description": "Dev"
        },
        "changeDescription": "some-change-description"
    }
}
