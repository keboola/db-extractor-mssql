<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Traits;

use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;

trait ConfigTrait
{
    private function getConfig(): array
    {
        $configTemplate = <<<JSON
{
  "parameters": {
    "db": %s,
    "tables": [
      {
        "id": 1,
        "name": "sales",
        "query": "SELECT * FROM sales",
        "outputTable": "in.c-main.sales",
        "incremental": false,
        "primaryKey": null,
        "enabled": true
      },
      {
        "id": 2,
        "enabled": true,
        "name": "tablecolumns",
        "outputTable": "in.c-main.tablecolumns",
        "incremental": false,
        "primaryKey": null,
        "table": {
          "schema": "dbo",
          "tableName": "sales"
        },
        "columns": [
          "usergender",
          "usercity",
          "usersentiment",
          "zipcode"
        ]
      },
      {
        "id": 3,
        "enabled": true,
        "name": "auto-increment-timestamp",
        "outputTable": "in.c-main.auto-increment-timestamp",
        "incremental": false,
        "primaryKey": ["_Weir%%d I-D"],
        "table": {
          "schema": "dbo",
          "tableName": "auto Increment Timestamp"
        }
      },
      {
        "id": 4,
        "enabled": true,
        "name": "special",
        "outputTable": "in.c-main.special",
        "incremental": false,
        "primaryKey": null,
        "table": {
          "schema": "dbo",
          "tableName": "special"
        }
      }
    ]
  }
}
JSON;
        return (array) json_decode(
            sprintf($configTemplate, json_encode(PdoTestConnection::getDbConfigArray())),
            true,
        );
    }

    public function getRowConfig(): array
    {
        $configTemplate = <<<JSON
{
  "parameters": {
    "db": %s,
    "name": "special",
    "outputTable": "in.c-main.special",
    "incremental": false,
    "primaryKey": null,
    "table": {
      "schema": "dbo",
      "tableName": "special"
    }
  }
}
JSON;
        return (array) json_decode(
            sprintf($configTemplate, json_encode(PdoTestConnection::getDbConfigArray())),
            true,
        );
    }
}
