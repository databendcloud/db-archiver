# db-archiver
A simple tool to archive databases to Databend.

## Supported data sources
| DataSources |  Supported  |
|:------------|:-----------:|
| MySQL       |     Yes     |
| PostgreSQL  |     Yes     |
| TiDB        |     Yes     |
| SQL Server  |     Yes     |
| Oracle      | Coming soon |
| CSV         | Coming soon |
| NDJSON      | Coming soon |


## Installation
Download the binary from [release page](https://github.com/databendcloud/db-archiver/releases) according to your arch.

## Usage

Config your database and Databend connection in `config/conf.json`:
```json
{
  "sourceHost": "127.0.0.1",
  "sourcePort": 3306,
  "sourceUser": "root",
  "sourcePass": "123456",
  "sourceDbTables": ["mydb.*@table.*"],
  "sourceQuery": "select * from mydb.my_table",
  "sourceWhereCondition": "id < 100",
  "sourceSplitKey": "id",
  "databendDSN": "http://username:password@host:port",
  "databendTable": "testSync.my_table",
  "batchSize": 20000,
  "batchMaxInterval": 30,
  "workers": 1,
  "copyPurge": true,
  "copyForce": false,
  "disableVariantCheck": true,
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
| Parameter            | Description              | Default  | example                       | required |
|----------------------|--------------------------|----------|-------------------------------|----------|
| sourceHost           | source host              |          |                               | true     |
| sourcePort           | source port              | 3306     | 3306                          | true     |
| sourceUser           | source user              |          |                               | true     |
| sourcePass           | source password          |          |                               | true     |
| sourceDB             | source database          |          |                               | false     |
| sourceTable          | source table             |          |                               | false     |
| sourceDbTables       | source db tables         | []       | [db.*@table.*,mydb.*.table.*] | false    |
| sourceQuery          | source query             |          |                               | false     |
| sourceWhereCondition | source where condition   |          |                               | false    |
| sourceSplitKey       | source split key         | no       | "id"                          | false    |
| sourceSplitTimeKey   | source split time key    | no       | "t1"                          | false    |
| timeSplitUnit        | time split unit          | "minute" | "day"                         | false    |
| databendDSN          | databend dsn             | no       | "http://localhost:8000"       | true     |
| databendTable        | databend table           | no       | "db1.tbl"                     | true     |
| batchSize            | batch size               | 1000     | 1000                          | false    |
| copyPurge            | copy purge               | false    | false                         | false    |
| copyForce            | copy force               | false    | false                         | false    |
| DisableVariantCheck  | disable variant check    | false    | false                         | false    |
| userStage            | user external stage name | ~        | ~                             | false    |

NOTE: 1. To reduce the server load, we set the `sourceSplitKey` which is the primary key of the source table. The tool will split the data by the `sourceSplitKey` and sync the data to Databend in parallel.
The `sourceSplitTimeKey` is used to split the data by the time column. And the `sourceSplitTimeKey` and `sourceSplitKey` must be set at least one.
2. `sourceDbTables` is used to sync the data from multiple tables. The format is `db.*@table.*` or `db.table.*`. The `.*` is a regex pattern. The `db.*@table.*` means all tables match the regex pattern `table.*` in the database match the regex pattern `db.*`. 
3. `sourceDbTables` has a higher priority than `sourceTable` and `sourceDB`. If `sourceDbTables` is set, the `sourceTable` will be ignored.
4. The `database` and `table` all support regex pattern. 
5. If you set `sourceDbTables` the `sourceQuery` no need to set. In other words, the proirity of `sourceDbTables` if high than `sourceQuery`.


## Two modes
### Sync data according to the `sourceSplitKey`
If your source table has a primary key, you can set the `sourceSplitKey` to sync the data in parallel. The tool will split the data by the `sourceSplitKey` and sync the data to Databend in parallel.
It is the most high performance mode.
Th example of the `conf.json`:
```json
{
  "sourceHost": "0.0.0.0",
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
  "deleteAfterSync": false,
  "maxThread": 10
}
```

### Sync data according to the `sourceSplitTimeKey`
If your source table has a time column, you can set the `sourceSplitTimeKey` to sync the data in parallel. The tool will split the data by the `sourceSplitTimeKey` and sync the data to Databend in parallel.
The `sourceSplitTimeKey` must be set with `timeSplitUnit`. The `timeSplitUnit` can be `minute`, `hour`, `day`. The `timeSplitUnit` is used to split the data by the time column.
The example of the `conf.json`:
```json
 "sourceHost": "127.0.0.1",
  "sourcePort": 3306,
  "sourceUser": "root",
  "sourcePass": "12345678",
  "sourceDB": "mydb",
  "sourceTable": "test_table1",
  "sourceQuery": "select * from mydb.test_table1",
  "sourceWhereCondition": "t1 >= '2024-06-01' and t1 < '2024-07-01'",
  "sourceSplitKey": "",
  "sourceSplitTimeKey": "t1",
  "timeSplitUnit": "hour",
  "databendDSN": "https://cloudapp:password@tn3ftqihs--medium-p8at.gw.aws-us-east-2.default.databend.com:443",
  "databendTable": "default.test_table1",
  "batchSize": 10000,
  "batchMaxInterval": 30,
  "copyPurge": true,
  "copyForce": false,
  "disableVariantCheck": true,
  "userStage": "~",
  "deleteAfterSync": false,
  "maxThread": 10
```
NOTE:
1. If you set `sourceSplitTimeKey` the `sourceWhereCondition` format must be `t > xx and t < yy`.


NOTE: The `mysql-go` will handle the bool type as TINYINT(1). So you need to use `TINYINT` in databend to store the bool type.
