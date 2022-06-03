# MS SQL DB Extractor

[![Build Status](https://travis-ci.com/keboola/db-extractor-mssql.svg?branch=master)](https://travis-ci.com/keboola/db-extractor-mssql)

## Extractor DB Account Setup

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

## Configuration

    {
      "db": {
        "driver": "mssql",
        "host": "HOST",
        "port": "PORT",
        "database": "DATABASE",
        "user": "USERNAME",
        "password": "PASSWORD",
        "ssh": {
          "enabled": true,
          "keys": {
            "private": "ENCRYPTED_PRIVATE_SSH_KEY",
            "public": "PUBLIC_SSH_KEY"
          },
          "sshHost": "PROXY_HOSTNAME"
        }
      },
      "tables": [
        {
          "name": "employees",
          "query": "SELECT * FROM employees",
          "outputTable": "in.c-main.employees",
          "incremental": false,
          "enabled": true,
          "primaryKey": null
        }
      ]
    }

## License

MIT licensed, see [LICENSE](./LICENSE) file.
