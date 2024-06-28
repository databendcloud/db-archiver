# db-archiver
A simple tool to archive databases to Databend.


## Installation
```bash
go install github.com/databend/db-archiver@latest
```

## Usage

Config your database and Databend connection in `config/conf.json`:
```json
{
  "sourceHost": "127.0.0.1",
  "sourcePort": 3306,
  "sourceUser": "root",
  "sourcePass": "123456",
  "sourceDB": "mydb",
  "sourceTable": "my_table",
  "sourceQuery": "select * from mydb.my_table",
  "sourceWhereCondition": "id < 100",
  "sourceSplitKey": "id",
  "databendDSN": "https://cloudapp:password@tn3ftqihs--medium-p8at.gw.aws-us-east-2.default.databend.com:443",
  "databendTable": "testSync.my_table",
  "batchSize": 2,
  "batchMaxInterval": 30,
  "workers": 1,
  "copyPurge": false,
  "copyForce": false,
  "disableVariantCheck": false,
  "userStage": "~",
  "deleteAfterSync": false
}

```

Run the tool and start your sync:
```bash
./db-archiver -f conf.json
```

The log output:
```
INFO[0000] Starting worker              
2024/06/25 11:35:37 ingest 2 rows (0.565646 rows/s), 64 bytes (18.100678 bytes/s)
2024/06/25 11:35:38 ingest 1 rows (0.556652 rows/s), 33 bytes (17.812853 bytes/s)
2024/06/25 11:35:38 ingest 2 rows (0.551906 rows/s), 65 bytes (17.660995 bytes/s)
2024/06/25 11:35:38 ingest 2 rows (0.531644 rows/s), 64 bytes (17.012600 bytes/s)
2024/06/25 11:35:38 ingest 2 rows (0.531768 rows/s), 64 bytes (17.016584 bytes/s)
```


## Parameter References
| Parameter             | Description              | Default | example                 | required   |
|-----------------------|--------------------------|---------|-------------------------|------------|
| sourceHost            | source host              |         |                         | true       |
| sourcePort            | source port              | 3306    | 3306                    | true |
| sourceUser            | source user              |         |                         | true|
| sourcePass            | source password          |         |                         | true                    |
| sourceDB              | source database          |         |                         | true                    |
| sourceTable           | source table             |         |                         | true                    |
| sourceQuery           | source query             |         |                         | true                    |
| sourceWhereCondition  | source where condition   |         |                         | false                   |
| sourceSplitKey        | source split key         | no      | "id"                    | true       |
| databendDSN           | databend dsn             | no      | "http://localhost:8000" | true       |
| databendTable         | databend table           | no      | "db1.tbl"               | true       |
| batchSize             | batch size               | 1000    | 1000                    | false      |
| batchMaxInterval      | batch max interval       | 30      | 30                      | false      |
| copyPurge             | copy purge               | false   | false                   | false      |
| copyForce             | copy force               | false   | false                   | false      |
| DisableVariantCheck   | disable variant check    | false   | false                   | false      |
| userStage             | user external stage name | ~       | ~                       | false      |

NOTE: To reduce the server load, we set the `sourceSplitKey` which is the primary key of the source table. The tool will split the data by the `sourceSplitKey` and sync the data to Databend in parallel.
