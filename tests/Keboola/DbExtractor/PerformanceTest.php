<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;
use Keboola\DbExtractor\MSSQLApplication;

class PerformanceTest extends AbstractMSSQLTest
{

    private function cleanupTestSchemas(int $numberOfSchemas, int $numberOfTablesPerSchema): void
    {
        // cleanup
        for ($schemaCount = 0; $schemaCount < $numberOfSchemas; $schemaCount++) {
            for ($tableCount = 0; $tableCount < $numberOfTablesPerSchema; $tableCount++) {
                $this->pdo->exec(
                    sprintf(
                        "IF OBJECT_ID('testschema_%d.testtable_%d', 'U') IS NOT NULL ALTER TABLE [testschema_%d].[testtable_%d] DROP CONSTRAINT pk_%d_%d",
                        $schemaCount,
                        $tableCount,
                        $schemaCount,
                        $tableCount,
                        $schemaCount,
                        $tableCount
                    )
                );
                $this->dropTable(
                    sprintf("testtable_%d", $tableCount),
                    sprintf("testschema_%d", $schemaCount)
                );
            }
            $this->pdo->exec(sprintf("DROP SCHEMA IF EXISTS [testschema_%d]", $schemaCount));
        }
    }

    public function testThousandsOfTablesGetTables(): void
    {
        // $this->markTestSkipped("No need to run this test every time.");
        $testStartTime = time();
        $numberOfSchemas = 5;
        $numberOfTablesPerSchema = 100;
        $numberOfColumnsPerTable = 50;
        $maxRunTime = 5;

        $this->cleanupTestSchemas($numberOfSchemas, $numberOfTablesPerSchema);

        // gen columns
        $columnsSql = "";
        for ($columnCount = 0; $columnCount < $numberOfColumnsPerTable; $columnCount++) {
            $columnsSql .= sprintf(", [col_%d] VARCHAR(50) NOT NULL DEFAULT ''", $columnCount);
        }

        for ($schemaCount = 0; $schemaCount < $numberOfSchemas; $schemaCount++) {
            $this->pdo->exec(sprintf("CREATE SCHEMA [testschema_%d]", $schemaCount));
            for ($tableCount = 0; $tableCount < $numberOfTablesPerSchema; $tableCount++) {
                $this->pdo->exec(
                    sprintf(
                        "CREATE TABLE [testschema_%d].[testtable_%d] ([ID] INT IDENTITY(1,1) NOT NULL%s, CONSTRAINT pk_%d_%d PRIMARY KEY ([ID]))",
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
        $config['action'] = 'getTables';
        $app = $this->createApplication($config);

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

    public function testLargeTableRun(): void
    {
        $this->markTestSkipped("This is a work in progress.");

        $insertionScript = <<<EOT
Declare @Id int
Set @Id = 1

While @Id <= 1000000
Begin 
   Insert Into largetest values ('One morning, when Gregor Samsa woke from troubled dreams, 
he found himself transformed in his bed into a horrible vermin. 
He lay on his armour-like back, and if he lifted his head a little he could see his brown belly, 
slightly domed and divided by a')
   
   Set @Id = @Id + 1
End
EOT;

        $this->dropTable("largetest");

        $this->pdo->exec("CREATE TABLE largetest (id int identity primary key, kafka VARCHAR(255))");

        $this->pdo->exec($insertionScript);

        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables']);
        unset($config['parameters']['tables']);
        $config['parameters']['tables'][] = [
            'id' => 1,
            'name' => 'largetest',
            'outputTable' => 'in.c-main.largetest',
            'table' => [
                'tableName' => 'largetest',
                'schema' => 'dbo',

            ],
        ];

        $app = $this->createApplication($config);
        $startTime = time();
        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $runTime = time() - $startTime;

        echo "\nThe app ran in " . $runTime . " seconds.\n";

        $this->dropTable('largetest');
    }
}