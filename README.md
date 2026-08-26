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
- `incrementalFetchingMode` _(optional)_ – string: `watermark` (default) or `window` (see [Incremental fetching modes](#incremental-fetching-modes))
- `incrementalFetchingLookback` _(optional)_ – string: watermark mode only; re-fetch this far behind the last value
- `incrementalFetchingStart` _(optional)_ – string: window mode only; lower bound of the range
- `incrementalFetchingEnd` _(optional)_ – string: window mode only; upper bound of the range
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

### Incremental fetching modes

When `incrementalFetchingColumn` is set, the extractor supports two modes selected by `incrementalFetchingMode`:

- **`watermark`** (default) – resumes from the stored watermark (`column >= lastFetchedRow`). Optionally set `incrementalFetchingLookback` to also re-scan a margin *behind* the last value, so a row that was committed late (assigned a value below the watermark but only visible after the watermark had already advanced past it) is still picked up. The lookback is a duration for datetime columns (e.g. `"20 minutes"`) or a number for numeric columns (e.g. `"100"`); it is subtracted from the watermark.
- **`window`** – fetches a fixed range, `column >= incrementalFetchingStart [AND column <= incrementalFetchingEnd]`, **ignoring** the stored watermark. Bounds may be relative (e.g. `"2 days ago"`) or absolute (e.g. `"2024-01-01"`) for datetime columns, or numbers for numeric columns. Either bound may be omitted.

Notes:
- The two modes are mutually exclusive: `incrementalFetchingLookback` is read only in `watermark` mode, and `incrementalFetchingStart`/`incrementalFetchingEnd` only in `window` mode. Leftover keys from the other mode are ignored. Omitting `incrementalFetchingMode` (and `incrementalFetchingLookback`) keeps the classic watermark behavior unchanged.
- A `rowversion`/`timestamp` (binary) incremental column supports plain `watermark` fetching only — a lookback or window fails with a clear "not supported" error, because a monotonic binary token has no meaningful lower bound.
- A lookback, or a `window` `start`, re-fetches rows that may already be in Storage. With incremental *loading* enabled, set `primaryKey` so those rows are deduplicated. An absolute window `end` caps the fetched range and logs a warning (rows committed after it are never picked up by later runs) — expected for a one-off/segmented backfill, not for ongoing sync.

## Development

Clone this repository and initialize the workspace with the following commands:

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
