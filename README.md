# MS SQL DB Extractor

## Configuration

The configuration `config.json` contains following properties in `parameters` key:

- `db`
    - `host` – string
    - `port` _(optional)_ – int (default `1433`)
    - `database` – string
    - `user` – string
    - `#password` – string
    - `ssh` _(optional)_ – object: Settings for SSH tunnel
        - `enabled` – bool
        - `sshHost` – string: IP address or hostname of SSH server
        - `sshPort` _(optional)_ – int: SSH server port (default `22`)
        - `localPort` _(optional)_ – int: SSH tunnel local port in Docker container (default `33006`)
        - `user` _(optional)_ – string: SSH user (default same as `db.user`)
        - `compression` _(optional)_ - bool: Enables SSH tunnel compression (default `false`)
        - `keys` _(optional)_ – SSH keys
            - `public` – string: Public SSH key
            - `#private` – string: Private SSH key
    - `ssl` _(optional)_ – object
        - `enabled` _(optional)_ – bool (default `false`)
        - `ca` – string: Certificate file
        - `verifyServerCert` – bool
        - `ignoreCertificateCn` _(optional)_ – bool (default `false`)
- `enabled` _(optional)_ – bool (default `true`)
- `name` _(optional)_ – string
- `query` _(optional)_ – string (just one of `query` or `table` must be set)
- `table` _(optional)_ – object (just one of `query` or `table` must be set)
    - `schema` – string
    - `tableName` – string
- `columns` _(optional)_ – array of strings
- `outputTable` – string
- `incremental` _(optional)_ – bool (default `false`)
- `incrementalFetchingColumn` _(optional)_ – string
- `incrementalFetchingLimit` _(optional)_ – int
- `primaryKey` _(optional)_ – array of strings
- `retries` _(optional)_ – int: Number of PDO (fallback) retries if an error occurred (default `5`)
- `maxTriesBcp` _(optional)_ – int: Number of BCP retries if an error occurred (default `1`)
- `nolock` _(optional)_ – bool (default `false`)
- `disableBcp` _(optional)_ – bool: Do not use BCP for export (default `false`)
- `disableFallback` _(optional)_ – bool: Do not use PDO fallback for export (default `false`)
- `nolock` _(optional)_ – bool (default `false`)
- `cdcMode` _(optional)_ – bool (default `false`)
- `cdcModeFullLoadFallback` _(optional)_ – bool (default `false`)
- `queryTimeout` _(optional)_ – int: Number of seconds after which BCP as well as PDO exports will time out (default `null`)

## Development

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/db-extractor-mssql
cd db-extractor-mssql
docker compose build
docker compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:

```
docker compose run --rm dev composer tests
```

### Setup test database

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
