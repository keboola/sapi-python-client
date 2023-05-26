list_response = [
    {
        "id": "keboola.python-transformation-v2",
        "type": "transformation",
        "name": "Python",
        "description": "A programming language that lets you work quickly\nand integrate systems more effectively.",
        "longDescription": "Application which runs KBC transformations written in Python.",
        "version": 80,
        "complexity": None,
        "categories": [],
        "hasUI": False,
        "hasRun": False,
        "ico32": "https://ui.keboola-assets.com/developer-portal/icons/keboola.python-transformation-v2/32"
                 "/1666646281880.png",
        "ico64": "https://ui.keboola-assets.com/developer-portal/icons/keboola.python-transformation-v2/64"
                 "/1666646281880.png",
        "ico128": "https://ui.keboola-assets.com/developer-portal/icons/keboola.python-transformation-v2/128"
                  "/1666646281880.png",
        "data": {
            "definition": {
                "type": "aws-ecr",
                "uri": "147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2"
                       "/keboola.python-transformation-v2",
                "tag": "1.3.4",
                "digest": "sha256:75cee6c754fc10960c010a888ef7fec4fc8493d9d9c2b2b98514a3b8b480eb10"
            },
            "vendor": {
                "contact": [
                    "Keboola :(){:|:&};: s.r.o.",
                    "Dělnická 191/27\nHolešovice\n170 00 Praha 7",
                    "support@keboola.com"
                ],
                "licenseUrl": "https://www.keboola.com/terms-and-conditions"
            },
            "configuration_format": "json",
            "network": "bridge",
            "memory": "16000m",
            "process_timeout": 57600,
            "forward_token": False,
            "forward_token_details": False,
            "default_bucket": False,
            "staging_storage": {
                "input": "local",
                "output": "local"
            }
        },
        "flags": [
            "genericDockerUI",
            "genericDockerUI-tableInput",
            "genericDockerUI-fileInput",
            "genericDockerUI-tableOutput",
            "genericDockerUI-fileOutput",
            "genericCodeBlocksUI",
            "genericVariablesUI",
            "genericPackagesUI"
        ],
        "configurationSchema": {},
        "configurationRowSchema": {},
        "emptyConfiguration": {},
        "emptyConfigurationRow": {},
        "uiOptions": {},
        "configurationDescription": None,
        "features": [
            "allow-tag-override",
            "mlflow-artifacts-access"
        ],
        "expiredOn": None,
        "uri": "https://syrup.keboola.com/docker/keboola.python-transformation-v2",
        "documentationUrl": "https://help.keboola.com/transformations/python-plain/",
        "configurations": [
            {
                "id": "977780310",
                "name": "some other",
                "description": "",
                "created": "2023-05-18T08:05:49+0200",
                "creatorToken": {
                    "id": 373115,
                    "description": "support@keboola.com"
                },
                "version": 11,
                "changeDescription": "Update output table out.c-some-other.someothertable",
                "isDisabled": False,
                "isDeleted": False,
                "currentVersion": {
                    "created": "2023-05-18T08:11:06+0200",
                    "creatorToken": {
                        "id": 373115,
                        "description": "support@keboola.com"
                    },
                    "changeDescription": "Update output table out.c-some-other.someothertable"
                }
            }
        ]
    },
    {
        "id": "keboola.redshift-transformation",
        "type": "transformation",
        "name": "Redshift SQL",
        "description": "The fast, fully managed, petabyte-scale data warehouse solution handles large scale data sets "
                       "and database migrations.",
        "longDescription": "Application which runs KBC transformations",
        "version": 23,
        "complexity": None,
        "categories": [],
        "hasUI": False,
        "hasRun": False,
        "ico32": "https://ui.keboola-assets.com/developer-portal/icons/keboola.redshift-transformation/32"
                 "/1600778122782.png",
        "ico64": "https://ui.keboola-assets.com/developer-portal/icons/keboola.redshift-transformation/64"
                 "/1600778122782.png",
        "ico128": None,
        "data": {
            "definition": {
                "type": "aws-ecr",
                "uri": "147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2"
                       "/keboola.redshift-transformation",
                "tag": "0.3.0",
                "digest": "sha256:93ac608264acd2b267eebaf0f61f243aa104093b7fb2e3b7366c6de1eff59488"
            },
            "vendor": {
                "contact": [
                    "Keboola :(){:|:&};: s.r.o.",
                    "Dělnická 191/27\nHolešovice\n170 00 Praha 7",
                    "support@keboola.com"
                ],
                "licenseUrl": "https://www.keboola.com/terms-and-conditions"
            },
            "configuration_format": "json",
            "network": "bridge",
            "process_timeout": 36000,
            "forward_token": False,
            "forward_token_details": False,
            "default_bucket": False,
            "staging_storage": {
                "input": "workspace-redshift",
                "output": "workspace-redshift"
            }
        },
        "flags": [
            "genericDockerUI",
            "genericDockerUI-tableInput",
            "genericDockerUI-tableOutput",
            "genericCodeBlocksUI",
            "genericVariablesUI"
        ],
        "configurationSchema": {},
        "configurationRowSchema": {},
        "emptyConfiguration": {},
        "emptyConfigurationRow": {},
        "uiOptions": {},
        "configurationDescription": None,
        "features": [],
        "expiredOn": None,
        "uri": "https://syrup.keboola.com/docker/keboola.redshift-transformation",
        "documentationUrl": "https://help.keboola.com/transformations/redshift-plain/",
        "configurations": [
            {
                "id": "834914757",
                "name": "test",
                "description": "",
                "created": "2022-04-04T13:23:08+0200",
                "creatorToken": {
                    "id": 373115,
                    "description": "support@keboola.com"
                },
                "version": 8,
                "changeDescription": "Change Main",
                "isDisabled": False,
                "isDeleted": False,
                "currentVersion": {
                    "created": "2022-04-04T14:24:05+0200",
                    "creatorToken": {
                        "id": 373115,
                        "description": "support@keboola.com"
                    },
                    "changeDescription": "Change Main"
                }
            }
        ]
    },
]

branch_list_response = [
    {
        "id": "keboola.python-transformation-v2",
        "type": "transformation",
        "name": "Python",
        "description": "A programming language that lets you work quickly\nand integrate systems more effectively.",
        "longDescription": "Application which runs KBC transformations written in Python.",
        "version": 80,
        "complexity": None,
        "categories": [],
        "hasUI": False,
        "hasRun": False,
        "ico32": "https://ui.keboola-assets.com/developer-portal/icons/keboola.python-transformation-v2/32"
                 "/1666646281880.png",
        "ico64": "https://ui.keboola-assets.com/developer-portal/icons/keboola.python-transformation-v2/64"
                 "/1666646281880.png",
        "ico128": "https://ui.keboola-assets.com/developer-portal/icons/keboola.python-transformation-v2/128"
                  "/1666646281880.png",
        "data": {
            "definition": {
                "type": "aws-ecr",
                "uri": "147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2"
                       "/keboola.python-transformation-v2",
                "tag": "1.3.4",
                "digest": "sha256:75cee6c754fc10960c010a888ef7fec4fc8493d9d9c2b2b98514a3b8b480eb10"
            },
            "vendor": {
                "contact": [
                    "Keboola :(){:|:&};: s.r.o.",
                    "Dělnická 191/27\nHolešovice\n170 00 Praha 7",
                    "support@keboola.com"
                ],
                "licenseUrl": "https://www.keboola.com/terms-and-conditions"
            },
            "configuration_format": "json",
            "network": "bridge",
            "memory": "16000m",
            "process_timeout": 57600,
            "forward_token": False,
            "forward_token_details": False,
            "default_bucket": False,
            "staging_storage": {
                "input": "local",
                "output": "local"
            }
        },
        "flags": [
            "genericDockerUI",
            "genericDockerUI-tableInput",
            "genericDockerUI-fileInput",
            "genericDockerUI-tableOutput",
            "genericDockerUI-fileOutput",
            "genericCodeBlocksUI",
            "genericVariablesUI",
            "genericPackagesUI"
        ],
        "configurationSchema": {},
        "configurationRowSchema": {},
        "emptyConfiguration": {},
        "emptyConfigurationRow": {},
        "uiOptions": {},
        "configurationDescription": None,
        "features": [
            "allow-tag-override",
            "mlflow-artifacts-access"
        ],
        "expiredOn": None,
        "uri": "https://syrup.keboola.com/docker/keboola.python-transformation-v2",
        "documentationUrl": "https://help.keboola.com/transformations/python-plain/",
        "configurations": [
            {
                "id": "977780310",
                "name": "some other",
                "description": "",
                "created": "2023-05-18T08:05:49+0200",
                "creatorToken": {
                    "id": 373115,
                    "description": "support@keboola.com"
                },
                "version": 11,
                "changeDescription": "Update output table out.c-some-other.someothertable",
                "isDisabled": False,
                "isDeleted": False,
                "currentVersion": {
                    "created": "2023-05-18T08:11:06+0200",
                    "creatorToken": {
                        "id": 373115,
                        "description": "support@keboola.com"
                    },
                    "changeDescription": "Update output table out.c-some-other.someothertable"
                }
            }
        ]
    }
]
