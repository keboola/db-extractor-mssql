<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use Keboola\DbExtractor\MSSQLApplication;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use \PDO;

class PerformanceTest extends TestCase
{
    protected string $dataDir = __DIR__ . '/data';

    private PDO $pdo;

    protected function setUp(): void
    {
        $this->pdo = PdoTestConnection::createConnection();
    }

    private function cleanupTestSchemas(int $numberOfSchemas, int $numberOfTablesPerSchema): void
    {
        // cleanup
        for ($schemaCount = 0; $schemaCount < $numberOfSchemas; $schemaCount++) {
            for ($tableCount = 0; $tableCount < $numberOfTablesPerSchema; $tableCount++) {
                $this->pdo->exec(
                    sprintf(
                        "IF OBJECT_ID('testschema_%d.testtable_%d', 'U') IS NOT NULL " .
                        'ALTER TABLE [testschema_%d].[testtable_%d] DROP CONSTRAINT pk_%d_%d',
                        $schemaCount,
                        $tableCount,
                        $schemaCount,
                        $tableCount,
                        $schemaCount,
                        $tableCount
                    )
                );
                $removeTables = [
                    sprintf('testtable_%d', $tableCount),
                    sprintf('testschema_%d', $schemaCount)
                ];
                foreach ($removeTables as $removeTable) {
                    $this->pdo->exec(
                        sprintf(
                            "IF OBJECT_ID('[%s].[%s]', 'U') IS NOT NULL DROP TABLE [%s].[%s]",
                            'dbo',
                            $removeTable,
                            'dbo',
                            $removeTable
                        )
                    );
                }
            }
            $this->pdo->exec(sprintf('DROP SCHEMA IF EXISTS [testschema_%d]', $schemaCount));
        }
    }

    public function testThousandsOfTablesGetTables(): void
    {
        // $this->markTestSkipped("No need to run this test every time.");
        $testStartTime = time();
        $numberOfSchemas = 5;
        $numberOfTablesPerSchema = 100;
        $numberOfColumnsPerTable = 50;
        $maxRunTime = 10;

        $this->cleanupTestSchemas($numberOfSchemas, $numberOfTablesPerSchema);

        // gen columns
        $columnsSql = '';
        for ($columnCount = 0; $columnCount < $numberOfColumnsPerTable; $columnCount++) {
            $columnsSql .= sprintf(", [col_%d] VARCHAR(50) NOT NULL DEFAULT ''", $columnCount);
        }

        for ($schemaCount = 0; $schemaCount < $numberOfSchemas; $schemaCount++) {
            $this->pdo->exec(sprintf('CREATE SCHEMA [testschema_%d]', $schemaCount));
            for ($tableCount = 0; $tableCount < $numberOfTablesPerSchema; $tableCount++) {
                $this->pdo->exec(
                    sprintf(
                        'CREATE TABLE [testschema_%d].[testtable_%d] ' .
                        '([ID] INT IDENTITY(1,1) NOT NULL%s, CONSTRAINT pk_%d_%d PRIMARY KEY ([ID]))',
                        $schemaCount,
                        $tableCount,
                        $columnsSql,
                        $schemaCount,
                        $tableCount
                    )
                );
            }
        }

        $dbBuildTime = time() - $testStartTime;
        echo "\nTest DB built in  " . $dbBuildTime . " seconds.\n";

        $config = $this->getConfig();

        $logger = new Logger('ex-db-mssql-tests');
        $app = new MSSQLApplication($config, $logger, [], $this->dataDir);

        $jobStartTime = time();
        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $runTime = time() - $jobStartTime;

        $this->assertLessThan($maxRunTime, $runTime);

        echo "\nThe tables were fetched in " . $runTime . " seconds.\n";
        $this->cleanupTestSchemas($numberOfSchemas, $numberOfTablesPerSchema);
        $entireTime = time() - $testStartTime;
        echo "\nComplete test finished in  " . $entireTime . " seconds.\n";
    }

    private function getConfig()
    {
        $configTemplate = <<<JSON
{
  "action": "getTables",
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
        return json_decode(
            sprintf($configTemplate, json_encode(PdoTestConnection::getDbConfigArray())),
            true
        );
    }
}
