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
