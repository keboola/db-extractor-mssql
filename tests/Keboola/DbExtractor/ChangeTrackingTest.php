<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;

class ChangeTrackingTest extends AbstractMSSQLTest
{
    public function testChangeTracking(): void
    {
        $config = $this->getChangeTrackingConfig();
        $result = ($this->createApplication($config))->run();
        $outputFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.change-tracking',
                'rows' => 6,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);
        @unlink($outputFile);

        $this->pdo->exec('INSERT INTO [change Tracking] ([name]) VALUES (\'charles\')');
        $this->pdo->exec('INSERT INTO [change Tracking] ([name]) VALUES (\'william\')');
        $nextResult = ($this->createApplication($config, $result['state']))->run();
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $nextResult);
        $this->assertArrayHasKey('lastFetchedRow', $nextResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $nextResult['state']['lastFetchedRow']
        );
        $this->assertEquals(2, $nextResult['imported']['rows']);
    }

    public function testIncrementalFetchingLimit(): void
    {
        $config = $this->getChangeTrackingConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $result = ($this->createApplication($config))->run();
        $outputFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.change-tracking',
                // incrementalFetchingLimit is ignored because lastFetchedRow is not set
                'rows' => 6,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        @unlink($outputFile);

        $this->pdo->exec('INSERT INTO [change Tracking] ([name]) VALUES (\'charles\')');
        $this->pdo->exec('INSERT INTO [change Tracking] ([name]) VALUES (\'william\')');
        $this->pdo->exec('INSERT INTO [change Tracking] ([name]) VALUES (\'jack\')');
        $this->pdo->exec('INSERT INTO [change Tracking] ([name]) VALUES (\'john\')');
        $config['parameters']['incrementalFetchingLimit'] = 3;
        $nextResult = ($this->createApplication($config, $result['state']))->run();
        //check that output state contains expected information
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.change-tracking',
                'rows' => 3,
            ],
            $nextResult['imported']
        );
        $this->assertArrayHasKey('state', $nextResult);
        $this->assertArrayHasKey('lastFetchedRow', $nextResult['state']);
        $this->assertTrue(intval($result['state']['lastFetchedRow']) + 3 === intval($nextResult['state']['lastFetchedRow']));
    }

    public function testChangeTrackingInvalidColumn(): void
    {
        $config = $this->getChangeTrackingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'fakeCol';

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Column [fakeCol] specified for incremental fetching was not found in the table');
        ($this->createApplication($config))->run();
    }

    public function testChangeTrackingInvalidConfig(): void
    {
        $config = $this->getChangeTrackingConfig();
        $config['parameters']['query'] = 'SELECT * FROM change_tracking';
        unset($config['parameters']['table']);

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('Incremental fetching is not supported for advanced queries.');
        $app = $this->createApplication($config);
        $app->run();
    }

    public function testChangeTrackingNotEnabled(): void
    {
        $config = $this->getChangeTrackingConfig();
        $config['parameters']['table']['tableName'] = 'auto Increment Timestamp';
        $config['parameters']['incrementalFetchingColumn'] = '_Weir%d I-D';
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Change tracking for table [auto Increment Timestamp] is not enabled');
        $app = $this->createApplication($config);
        $app->run();
    }

    protected function getChangeTrackingConfig(): array
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'change Tracking',
            'schema' => 'dbo',
            'changeTracking' => true,
        ];
        $config['parameters']['incremental'] = true;
        $config['parameters']['nolock'] = true;
        $config['parameters']['name'] = 'change-tracking';
        $config['parameters']['outputTable'] = 'in.c-main.change-tracking';
        $config['parameters']['primaryKey'] = ['id'];
        $config['parameters']['incrementalFetchingColumn'] = 'id';
        return $config;
    }
}
