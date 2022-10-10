<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\JsonHelper;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use Keboola\DbExtractor\MSSQLApplication;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use PHPUnit\Framework\TestCase;
use \PDO;
use Psr\Log\Test\TestLogger;

class PerformanceTest extends TestCase
{
    public const NUMBER_OF_SCHEMAS = 5;
    public const NUMBER_OF_TABLES_PER_SCHEMA = 100;

    use ConfigTrait;
    use RemoveAllTablesTrait;

    protected string $dataDir = __DIR__ . '/data';

    protected PDO $connection;

    protected function setUp(): void
    {
        putenv('KBC_DATADIR=' . $this->dataDir);
        $this->connection = PdoTestConnection::createConnection();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanupTestSchemas();
    }

    private function cleanupTestSchemas(): void
    {
        $this->removeAllTables();
        for ($schemaCount = 0; $schemaCount < self::NUMBER_OF_SCHEMAS; $schemaCount++) {
            $this->connection->exec(sprintf('DROP SCHEMA IF EXISTS [testschema_%d]', $schemaCount));
        }
    }

    public function testThousandsOfTablesGetTables(): void
    {
        // $this->markTestSkipped("No need to run this test every time.");
        $testStartTime = time();
        $numberOfColumnsPerTable = 50;
        $maxRunTime = 10;

        // gen columns
        $columnsSql = '';
        for ($columnCount = 0; $columnCount < $numberOfColumnsPerTable; $columnCount++) {
            $columnsSql .= sprintf(", [col_%d] VARCHAR(50) NOT NULL DEFAULT ''", $columnCount);
        }

        for ($schemaCount = 0; $schemaCount < self::NUMBER_OF_SCHEMAS; $schemaCount++) {
            $this->connection->exec(sprintf('CREATE SCHEMA [testschema_%d]', $schemaCount));
            for ($tableCount = 0; $tableCount < self::NUMBER_OF_TABLES_PER_SCHEMA; $tableCount++) {
                $this->connection->exec(
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
        $config['action'] = 'getTables';

        JsonHelper::writeFile($this->dataDir . '/config.json', $config);

        $logger = new TestLogger();
        $app = new MSSQLApplication($logger);

        $jobStartTime = time();
        ob_start();
        $app->execute();
        $result = json_decode((string) ob_get_contents(), true);
        ob_end_clean();
        $this->assertEquals('success', $result['status']);
        $runTime = time() - $jobStartTime;

        $this->assertLessThan($maxRunTime, $runTime);

        echo "\nThe tables were fetched in " . $runTime . " seconds.\n";
        $entireTime = time() - $testStartTime;
        echo "\nComplete test finished in  " . $entireTime . " seconds.\n";
    }
}
