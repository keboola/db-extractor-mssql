<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;

class IncrementalFetchingTest extends AbstractMSSQLTest
{
    public function testIncrementalFetchingByDatetime(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'datetime';
        $result = ($this->createApplication($config))->run();
        $outputFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 6,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);
        @unlink($outputFile);
        sleep(2);
        // the next fetch should be just the last fetched row from last time because of >=
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);
        // assert that the state is unchanged
        $this->assertEquals($result['state'], $noNewRowsResult['state']);
        sleep(2);
        //now add a couple rows and run it again.
        $this->pdo->exec('INSERT INTO [auto Increment Timestamp] ([Weir%d Na-me]) VALUES (\'charles\'), (\'william\')');
        $newResult = ($this->createApplication($config, $result['state']))->run();
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
        $this->assertEquals(3, $newResult['imported']['rows']);
    }
    public function testIncrementalFetchingByAutoIncrement(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = '_Weir%d I-D';
        $config['parameters']['nolock'] = true;
        $result = ($this->createApplication($config))->run();
        $outputFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 6,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(6, $result['state']['lastFetchedRow']);
        unlink($outputFile);
        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);
        // assert that the state is unchanged
        $this->assertEquals($result['state'], $noNewRowsResult['state']);
        sleep(2);
        //now add a couple rows and run it again.
        $this->pdo->exec('INSERT INTO [auto Increment Timestamp] ([Weir%d Na-me]) VALUES (\'charles\'), (\'william\')');
        $newResult = ($this->createApplication($config, $result['state']))->run();
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(8, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(3, $newResult['imported']['rows']);
    }
    public function testIncrementalFetchingByDecimal(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'someDecimal';
        $config['parameters']['nolock'] = true;
        $result = ($this->createApplication($config))->run();
        $outputFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 6,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(6.6, $result['state']['lastFetchedRow']);
        unlink($outputFile);
        sleep(2);
        // the next fetch should be empty
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);
        // assert that the state is unchanged
        $this->assertEquals($result['state'], $noNewRowsResult['state']);
        sleep(2);
        //now add a couple rows and run it again.
        $this->pdo->exec('INSERT INTO [auto Increment Timestamp] ([Weir%d Na-me], [someDecimal]) VALUES (\'charles\', 12.2), (\'william\', 7.5)');
        $newResult = ($this->createApplication($config, $result['state']))->run();
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals('12.20', $newResult['state']['lastFetchedRow']);
        $this->assertEquals(3, $newResult['imported']['rows']);
    }
    public function testIncrementalFetchingBySmalldatetime(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'smalldatetime';
        $config['parameters']['nolock'] = true;
        $result = ($this->createApplication($config))->run();
        $outputFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 6,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals('2012-01-10 10:25:00', $result['state']['lastFetchedRow']);
        unlink($outputFile);
        sleep(2);
        // the next fetch should contain the last 2 rows since they have the same value
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(2, $noNewRowsResult['imported']['rows']);
        // assert that the state is unchanged
        $this->assertEquals($result['state'], $noNewRowsResult['state']);
        sleep(2);
        //now add a couple rows and run it again.
        $this->pdo->exec('INSERT INTO [auto Increment Timestamp] ([Weir%d Na-me], [smalldatetime]) VALUES (\'charles\', \'2012-01-10 10:55\'), (\'william\', \'2012-01-10 10:50\')');
        $newResult = ($this->createApplication($config, $result['state']))->run();
        //check that output state contains expected information (will contain the same last 2 rows as above, + 2 more
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals('2012-01-10 10:55:00', $newResult['state']['lastFetchedRow']);
        $this->assertEquals(4, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByTimestamp(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'timestamp';
        $config['parameters']['nolock'] = true;
        $result = ($this->createApplication($config))->run();
        $outputFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 6,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);
        unlink($outputFile);
        sleep(2);
        // the next fetch should contain the last row since each row has a unique value
        $noNewRowsResult = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);
        // assert that the state is unchanged
        $this->assertEquals($result['state'], $noNewRowsResult['state']);
        sleep(2);
        //now add a couple rows and run it again.
        $this->pdo->exec('INSERT INTO [auto Increment Timestamp] ([Weir%d Na-me], [smalldatetime]) VALUES (\'charles\', \'2012-01-10 10:55\'), (\'william\', \'2012-01-10 10:50\')');
        $newResult = ($this->createApplication($config, $result['state']))->run();
        //check that output state contains expected information (will contain the same last row as above, + 2 more
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertNotEmpty($newResult['state']['lastFetchedRow']);
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingLimit(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $result = ($this->createApplication($config))->run();
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(1, $result['state']['lastFetchedRow']);
        sleep(2);
        // since it's >= we'll set limit to 2 to fetch the second row also
        $config['parameters']['incrementalFetchingLimit'] = 2;
        $result = ($this->createApplication($config, $result['state']))->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);
    }

    /**
     * @dataProvider invalidColumnProvider
     */
    public function testIncrementalFetchingInvalidColumns(string $column, string $expectedExceptionMessage): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = $column;

        $this->expectException(UserException::class);
        $this->expectExceptionMessage($expectedExceptionMessage);
        ($this->createApplication($config))->run();
    }

    public function invalidColumnProvider(): array
    {
        return [
            'column does not exist' => [
                'fakeCol',
                'Column [fakeCol] specified for incremental fetching was not found in the table',
            ],
            'column exists but is not numeric nor datetime so should fail' => [
                'Weir%d Na-me',
                'Column [Weir%d Na-me] specified for incremental fetching is not numeric or datetime',
            ],
        ];
    }

    public function testIncrementalFetchingInvalidConfig(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['query'] = 'SELECT * FROM auto_increment_timestamp';
        unset($config['parameters']['table']);

        $this->expectException(ConfigUserException::class);
        $this->expectExceptionMessage('Incremental fetching is not supported for advanced queries.');
        $app = $this->createApplication($config);
        $app->run();
    }

    protected function getIncrementalFetchingConfig(): array
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'auto Increment Timestamp',
            'schema' => 'dbo',
        ];
        $config['parameters']['incremental'] = true;
        $config['parameters']['name'] = 'auto-increment-timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['primaryKey'] = ['_Weir%d I-D'];
        $config['parameters']['incrementalFetchingColumn'] = '_Weir%d I-D';
        return $config;
    }
}
