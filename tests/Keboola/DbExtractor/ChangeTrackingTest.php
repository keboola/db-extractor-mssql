<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use PDO;
use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;

class ChangeTrackingTest extends AbstractMSSQLTest
{
    public function tearDown(): void
    {
        $this->dropTable('change Tracking 3');
    }

    public function testChangeTracking(): void
    {
        $this->pdo->exec("INSERT INTO [change Tracking] (\"name\", Type, someInteger, someDecimal, smalldatetime) VALUES ('mario', 'plumber', 1, 1.1, '2012-01-10 10:00')");
        $this->pdo->exec("INSERT INTO [change Tracking] (\"name\", Type, someInteger, someDecimal, smalldatetime) VALUES ('luigi', 'plumber', 2, 2.2, '2012-01-10 10:05')");
        $this->pdo->exec("INSERT INTO [change Tracking] (\"name\", Type, someInteger, someDecimal, smalldatetime) VALUES ('toad', 'mushroom', 3, 3.3, '2012-01-10 10:10')");
        $this->pdo->exec("INSERT INTO [change Tracking] (\"name\", Type, someInteger, someDecimal, smalldatetime) VALUES ('princess', 'royalty', 4, 4.4, '2012-01-10 10:15')");

        $config = $this->getChangeTrackingConfig();
        $result = ($this->createApplication($config))->run();
        $outputFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';
        $version = $this->pdo
            ->query('SELECT MAX(cht.sys_change_version) version FROM CHANGETABLE(CHANGES [dbo].[change Tracking], 0) cht', PDO::FETCH_ASSOC)
            ->fetch()['version'];

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.change-tracking',
                'rows' => 4,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals($version, $result['state']['lastFetchedRow']);
        @unlink($outputFile);

        $this->pdo->exec("INSERT INTO [change Tracking] (\"name\", Type, someInteger, someDecimal, smalldatetime) VALUES ('wario', 'badguy', 5, 5.5, '2012-01-10 10:25')");
        $this->pdo->exec("INSERT INTO [change Tracking] (\"name\", Type, someInteger, someDecimal, smalldatetime) VALUES ('yoshi', 'horse?', 6, 6.6, '2012-01-10 10:25')");

        $result = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['imported']['rows']);
        $this->assertEquals(intval($version) + 2, intval($result['state']['lastFetchedRow']));
    }

    public function testChangeTrackingWithTwoPrimaryKeys(): void
    {
        $this->pdo->exec("INSERT INTO [change Tracking 2] (\"name\", someInteger) VALUES ('mario', 1)");
        $this->pdo->exec("INSERT INTO [change Tracking 2] (\"name\", someInteger) VALUES ('luigi', 2)");
        $this->pdo->exec("INSERT INTO [change Tracking 2] (\"name\", someInteger) VALUES ('toad', 3)");
        $this->pdo->exec("INSERT INTO [change Tracking 2] (\"name\", someInteger) VALUES ('princess', 4)");

        $config = $this->getChangeTrackingConfig();
        $config['parameters']['table']['tableName'] = 'change Tracking 2';
        $config['parameters']['name'] = 'change-tracking-2';
        $config['parameters']['outputTable'] = 'in.c-main.change-tracking-2';
        $result = ($this->createApplication($config))->run();
        $outputFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';
        $version = $this->pdo
            ->query('SELECT MAX(cht.sys_change_version) version FROM CHANGETABLE(CHANGES [dbo].[change Tracking 2], 0) cht', PDO::FETCH_ASSOC)
            ->fetch()['version'];

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.change-tracking-2',
                'rows' => 4,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals($version, $result['state']['lastFetchedRow']);
        @unlink($outputFile);

        $this->pdo->exec("INSERT INTO [change Tracking 2] (\"name\", someInteger) VALUES ('wario', 5)");
        $this->pdo->exec("INSERT INTO [change Tracking 2] (\"name\", someInteger) VALUES ('yoshi', 6)");

        $result = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['imported']['rows']);
        $this->assertEquals(intval($version) + 2, intval($result['state']['lastFetchedRow']));
    }

    public function testChangeTrackingForExisitngTable(): void
    {
        $this->pdo->exec(
            'CREATE TABLE [change Tracking 3] (
            id INT IDENTITY(1,1) NOT NULL,
            name VARCHAR (55) NOT NULL,
            someInteger INT,
            PRIMARY KEY (id, name)
            )'
        );

        $this->pdo->exec("INSERT INTO [change Tracking 3] (\"name\", someInteger) VALUES ('mario', 1)");
        $this->pdo->exec("INSERT INTO [change Tracking 3] (\"name\", someInteger) VALUES ('luigi', 2)");
        $this->pdo->exec("INSERT INTO [change Tracking 3] (\"name\", someInteger) VALUES ('toad', 3)");

        $this->pdo->exec('ALTER TABLE [dbo].[change Tracking 3] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)');

        $config = $this->getChangeTrackingConfig();
        $config['parameters']['table']['tableName'] = 'change Tracking 3';
        $config['parameters']['name'] = 'change-tracking-3';
        $config['parameters']['outputTable'] = 'in.c-main.change-tracking-3';
        $result = ($this->createApplication($config))->run();
        $outputFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';
        $version = $this->pdo
            ->query('SELECT MAX(cht.sys_change_version) version FROM CHANGETABLE(CHANGES [dbo].[change Tracking 3], 0) cht', PDO::FETCH_ASSOC)
            ->fetch()['version'];

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.change-tracking-3',
                'rows' => 3,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNull($version);
        @unlink($outputFile);

        $this->pdo->exec("INSERT INTO [change Tracking 3] (\"name\", someInteger) VALUES ('princess', 4)");
        $version = $this->pdo
            ->query('SELECT MAX(cht.sys_change_version) version FROM CHANGETABLE(CHANGES [dbo].[change Tracking 3], 0) cht', PDO::FETCH_ASSOC)
            ->fetch()['version'];

        $result = ($this->createApplication($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(1, $result['imported']['rows']);
        $this->assertNotNull($version);
    }

    public function testChangeTrackingNotEnabled(): void
    {
        $config = $this->getChangeTrackingConfig();
        $config['parameters']['table']['tableName'] = 'auto Increment Timestamp';

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Change tracking for table [auto Increment Timestamp] is not enabled');
        ($this->createApplication($config))->run();
    }

    public function testChangeTrackingIncrementalFetchingColumn(): void
    {
        $config = $this->getChangeTrackingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'id';

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Incremental fetching is enabled. Disable change tracking or incremental fetching');
        ($this->createApplication($config))->run();
    }

    public function testChangeTrackingIncrementalFetchingLimit(): void
    {
        $config = $this->getChangeTrackingConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Incremental fetching limit is not supported for change tracking');
        ($this->createApplication($config))->run();
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
        $config['parameters']['nolock'] = true;
        $config['parameters']['name'] = 'change-tracking';
        $config['parameters']['outputTable'] = 'in.c-main.change-tracking';
        $config['parameters']['primaryKey'] = ['id'];

        return $config;
    }
}
