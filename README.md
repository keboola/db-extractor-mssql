# MS SQL DB Extractor

## Supported MS SQL Versions
This component uses the Microsoft ODBC Driver for SQL Server, version `18.0.1.1-1`, and supports the following versions:
- SQL Server 2012
- SQL Server 2014
- SQL Server 2016
- SQL Server 2017
- SQL Server 2019
- SQL Server 2022

## Configuration

The `config.json` file contains the following properties within the `parameters` key:

- `db`
    - `host` – string
    - `port` _(optional)_ – int (default: `1433`)
    - `database` – string
    - `user` – string
    - `#password` – string
    - `ssh` _(optional)_ – object: Settings for the SSH tunnel
        - `enabled` – bool
        - `sshHost` – string: IP address or hostname of the SSH server
        - `sshPort` _(optional)_ – int: SSH server port (default: `22`)
        - `localPort` _(optional)_ – int: SSH tunnel local port in the Docker container (default: `33006`)
        - `user` _(optional)_ – string: SSH user (default same as `db.user`)
        - `compression` _(optional)_ - bool: Enables SSH tunnel compression (default: `false`)
        - `keys` _(optional)_ – SSH keys
            - `public` – string: Public SSH key
            - `#private` – string: Private SSH key
    - `ssl` _(optional)_ – object
        - `enabled` _(optional)_ – bool (default: `false`)
        - `ca` – string: Certificate file
        - `verifyServerCert` – bool
        - `ignoreCertificateCn` _(optional)_ – bool (default: `false`)
- `enabled` _(optional)_ – bool (default: `true`)
- `name` _(optional)_ – string
- `query` _(optional)_ – string (either `query` or `table` must be set)
- `table` _(optional)_ – object (either `query` or `table` must be set)
    - `schema` – string
    - `tableName` – string
- `columns` _(optional)_ – array of strings
- `outputTable` – string
- `incremental` _(optional)_ – bool (default: `false`)
- `incrementalFetchingColumn` _(optional)_ – string
- `incrementalFetchingLimit` _(optional)_ – int
- `primaryKey` _(optional)_ – array of strings
- `retries` _(optional)_ – int: Number of PDO (fallback) retries if an error occurs (default: `5`)
- `maxTriesBcp` _(optional)_ – int: Number of BCP retries if an error occurs (default: `1`)
- `nolock` _(optional)_ – bool (default: `false`)
- `disableBcp` _(optional)_ – bool: Do not use BCP for export (default: `false`)
- `disableFallback` _(optional)_ – bool: Do not use PDO fallback for export (default: `false`)
- `nolock` _(optional)_ – bool (default `false`)
- `cdcMode` _(optional)_ – bool (default `false`)
- `cdcModeFullLoadFallback` _(optional)_ – bool (default `false`)
- `queryTimeout` _(optional)_ – int: Number of seconds before BCP and PDO exports time out (default: `null`)

## Development

Clone this repository and initialize the workspace with the following commands:

```
git clone https://github.com/keboola/db-extractor-mssql
cd db-extractor-mssql
export MSSQL_VERSION="2022"
docker compose build
docker compose run --rm dev composer install --no-scripts
```

### On ARM
```
export DOCKER_DEFAULT_PLATFORM=linux/amd64
docker compose build --build-arg TARGETPLATFORM=linux/arm64
```

Run the test suite using this command:

```
docker compose run --rm dev composer tests
```

### Setup Test Database

```
CREATE LOGIN tests_mssql_extractor WITH PASSWORD = '';
CREATE USER tests_mssql_extractor FOR LOGIN tests_mssql_extractor;
CREATE DATABASE tests_mssql_extractor;
USE tests_mssql_extractor;
CREATE SCHEMA tests;
CREATE TABLE tests.test (id text null, name text null);
INSERT INTO tests.test VALUES ('1', 'martin');
GRANT SELECT ON SCHEMA :: [tests] TO tests_mssql_extractor;
```
