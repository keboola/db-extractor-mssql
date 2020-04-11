<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;

class MSSQLTest extends AbstractMSSQLTest
{
    public function testCredentials(): void
    {
        $config = $this->getConfig('mssql');
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testCredentialsWrongDb(): void
    {
        $config = $this->getConfig('mssql');
        $config['parameters']['db']['database'] = 'nonExistentDb';
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];

        $app = $this->createApplication($config);
        try {
            $app->run();
            $this->fail('Must raise exception');
        } catch (UserException $e) {
            $this->assertStringContainsString('Cannot open database "nonExistentDb" requested by the login.', $e->getMessage());
        }
    }

    public function testRunWithoutTables(): void
    {
        $config = $this->getConfig('mssql');

        $config['parameters']['tables'] = [];

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunNoRows(): void
    {
        $salesManifestFile = $this->dataDir . '/out/tables/in.c-main.sales.csv.manifest';
        $salesDataFile = $this->dataDir . '/out/tables/in.c-main.sales.csv';
        @unlink($salesDataFile);
        @unlink($salesManifestFile);

        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);

        $config['parameters']['tables'][0]['query'] = "SELECT * FROM sales WHERE usergender LIKE 'undefined'";

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);

        $this->assertFileNotExists($salesManifestFile);
        $this->assertFileNotExists($salesDataFile);
    }

    /**
     * @dataProvider configProvider
     */
    public function testRunConfig(array $config): void
    {
        $result = $this->createApplication($config)->run();
        if (array_key_exists('tables', $config['parameters'])) {
            $this->checkTablesResult($result);
        } else {
            $this->checkRowResult($result);
        }
    }

    private function checkRowResult(array $result): void
    {
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            array (
                'outputTable' => 'in.c-main.special',
                'rows' => 7,
            ),
            $result['imported']
        );

        $specialManifest = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv.manifest';
        $manifest = json_decode((string) file_get_contents($specialManifest), true);
        $this->assertEquals(
            array (
                'destination' => 'in.c-main.special',
                'incremental' => false,
                'metadata' =>
                    array (
                        0 =>
                            array (
                                'key' => 'KBC.name',
                                'value' => 'special',
                            ),
                        1 =>
                            array (
                                'key' => 'KBC.schema',
                                'value' => 'dbo',
                            ),
                        2 =>
                            array (
                                'key' => 'KBC.catalog',
                                'value' => 'test',
                            ),
                        3 =>
                            array (
                                'key' => 'KBC.type',
                                'value' => 'BASE TABLE',
                            ),
                    ),
                'column_metadata' =>
                    array (
                        'col1' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'text',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'STRING',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '2147483647',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'col1',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'col1',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => '1',
                                    ),
                            ),
                        'col2' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'text',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'STRING',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '2147483647',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'col2',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'col2',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => '2',
                                    ),
                            ),
                    ),
                'columns' =>
                    array (
                        0 => 'col1',
                        1 => 'col2',
                    ),
            ),
            $manifest
        );
    }

    private function checkTablesResult(array $result): void
    {
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            array (
                0 =>
                    array (
                        'outputTable' => 'in.c-main.sales',
                        'rows' => 100,
                    ),
                1 =>
                    array (
                        'outputTable' => 'in.c-main.tablecolumns',
                        'rows' => 100,
                    ),
                2 =>
                    array (
                        'outputTable' => 'in.c-main.auto-increment-timestamp',
                        'rows' => 6,
                    ),
                3 =>
                    array (
                        'outputTable' => 'in.c-main.special',
                        'rows' => 7,
                    ),
            ),
            $result['imported']
        );

        $salesManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';
        $manifest = json_decode((string) file_get_contents($salesManifestFile), true);
        $this->assertEquals(
            [
                'destination' => 'in.c-main.sales',
                'incremental' => false,
                'columns' =>
                    array (
                        0 => 'usergender',
                        1 => 'usercity',
                        2 => 'usersentiment',
                        3 => 'zipcode',
                        4 => 'sku',
                        5 => 'createdat',
                        6 => 'category',
                        7 => 'price',
                        8 => 'county',
                        9 => 'countycode',
                        10 => 'userstate',
                        11 => 'categorygroup',
                    ),
            ],
            $manifest
        );

        $tableColumnsManifest = $this->dataDir . '/out/tables/' . $result['imported'][1]['outputTable'] . '.csv.manifest';
        $manifest = json_decode((string) file_get_contents($tableColumnsManifest), true);
        $this->assertEquals(
            array (
                'destination' => 'in.c-main.tablecolumns',
                'incremental' => false,
                'metadata' =>
                    array (
                        0 =>
                            array (
                                'key' => 'KBC.name',
                                'value' => 'sales',
                            ),
                        1 =>
                            array (
                                'key' => 'KBC.schema',
                                'value' => 'dbo',
                            ),
                        2 =>
                            array (
                                'key' => 'KBC.catalog',
                                'value' => 'test',
                            ),
                        3 =>
                            array (
                                'key' => 'KBC.type',
                                'value' => 'BASE TABLE',
                            ),
                    ),
                'column_metadata' =>
                    array (
                        'usergender' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'text',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'STRING',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '2147483647',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'usergender',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'usergender',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 1,
                                    ),
                            ),
                        'usercity' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'text',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'STRING',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '2147483647',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'usercity',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'usercity',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 2,
                                    ),
                            ),
                        'usersentiment' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'text',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'STRING',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '2147483647',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'usersentiment',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'usersentiment',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 3,
                                    ),
                            ),
                        'zipcode' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'text',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'STRING',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '2147483647',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'zipcode',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'zipcode',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 4,
                                    ),
                            ),
                    ),
                'columns' =>
                    array (
                        0 => 'usergender',
                        1 => 'usercity',
                        2 => 'usersentiment',
                        3 => 'zipcode',
                    ),
            ),
            $manifest
        );

        $weirdManifest = $this->dataDir . '/out/tables/' . $result['imported'][2]['outputTable'] . '.csv.manifest';
        $manifest = json_decode((string) file_get_contents($weirdManifest), true);
        // assert the timestamp column has the correct date format
        $outputData = iterator_to_array(
            new CsvFile($this->dataDir . '/out/tables/' . $result['imported'][2]['outputTable'] . '.csv')
        );
        $this->assertEquals(1, (int) $outputData[0][2]);
        $this->assertEquals('1.10', $outputData[0][3]);
        $firstTimestamp = $outputData[0][5];
        // there should be no decimal separator present (it should be cast to datetime2(0) which does not include ms)
        $this->assertEquals(1, preg_match('/^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/', $firstTimestamp));
        $this->assertEquals(
            array (
                'destination' => 'in.c-main.auto-increment-timestamp',
                'incremental' => false,
                'primary_key' =>
                    array (
                        0 => 'Weir_d_I_D',
                    ),
                'metadata' =>
                    array (
                        0 =>
                            array (
                                'key' => 'KBC.name',
                                'value' => 'auto Increment Timestamp',
                            ),
                        1 =>
                            array (
                                'key' => 'KBC.schema',
                                'value' => 'dbo',
                            ),
                        2 =>
                            array (
                                'key' => 'KBC.catalog',
                                'value' => 'test',
                            ),
                        3 =>
                            array (
                                'key' => 'KBC.type',
                                'value' => 'BASE TABLE',
                            ),
                    ),
                'column_metadata' =>
                    array (
                        'Weir_d_I_D' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'int',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => false,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'INTEGER',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '10',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => '_Weir%d I-D',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'Weir_d_I_D',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => true,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 1,
                                    ),
                            ),
                        'Weir_d_Na_me' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'varchar',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => false,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'STRING',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '55',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.datatype.default',
                                        'value' => '(\'mario\')',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'Weir%d Na-me',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'Weir_d_Na_me',
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => true,
                                    ),
                                9 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 2,
                                    ),
                            ),
                        'someInteger' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'int',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'INTEGER',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '10',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'someInteger',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'someInteger',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 3,
                                    ),
                            ),
                        'someDecimal' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'decimal',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'NUMERIC',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '10,2',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'someDecimal',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'someDecimal',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 4,
                                    ),
                            ),
                        'type' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'varchar',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'STRING',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '55',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'type',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'type',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => true,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 5,
                                    ),
                            ),
                        'smalldatetime' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'smalldatetime',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'TIMESTAMP',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.default',
                                        'value' => '(NULL)',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'smalldatetime',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'smalldatetime',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 6,
                                    ),
                            ),
                        'datetime' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'datetime',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => false,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'TIMESTAMP',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.default',
                                        'value' => '(getdate())',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'datetime',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'datetime',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 7,
                                    ),
                            ),
                        'timestamp' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'timestamp',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => false,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'STRING',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'timestamp',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'timestamp',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 8,
                                    ),
                            ),
                    ),
                'columns' =>
                    array (
                        0 => 'Weir_d_I_D',
                        1 => 'Weir_d_Na_me',
                        2 => 'someInteger',
                        3 => 'someDecimal',
                        4 => 'type',
                        5 => 'smalldatetime',
                        6 => 'datetime',
                        7 => 'timestamp',
                    ),
            ),
            $manifest
        );

        $specialManifest = $this->dataDir . '/out/tables/' . $result['imported'][3]['outputTable'] . '.csv.manifest';
        $manifest = json_decode((string) file_get_contents($specialManifest), true);
        $this->assertEquals(
            array (
                'destination' => 'in.c-main.special',
                'incremental' => false,
                'metadata' =>
                    array (
                        0 =>
                            array (
                                'key' => 'KBC.name',
                                'value' => 'special',
                            ),
                        1 =>
                            array (
                                'key' => 'KBC.schema',
                                'value' => 'dbo',
                            ),
                        2 =>
                            array (
                                'key' => 'KBC.catalog',
                                'value' => 'test',
                            ),
                        3 =>
                            array (
                                'key' => 'KBC.type',
                                'value' => 'BASE TABLE',
                            ),
                    ),
                'column_metadata' =>
                    array (
                        'col1' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'text',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'STRING',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '2147483647',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'col1',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'col1',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => '1',
                                    ),
                            ),
                        'col2' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'text',
                                    ),
                                1 =>
                                    array (
                                        'key' => 'KBC.datatype.nullable',
                                        'value' => true,
                                    ),
                                2 =>
                                    array (
                                        'key' => 'KBC.datatype.basetype',
                                        'value' => 'STRING',
                                    ),
                                3 =>
                                    array (
                                        'key' => 'KBC.datatype.length',
                                        'value' => '2147483647',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'col2',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'col2',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => false,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => '2',
                                    ),
                            ),
                    ),
                'columns' =>
                    array (
                        0 => 'col1',
                        1 => 'col2',
                    ),
            ),
            $manifest
        );
    }

    public function testCredentialsWithSSH(): void
    {
        $config = $this->getConfig('mssql');
        $config['action'] = 'testConnection';

        $config['parameters']['db']['ssh'] = [
         'enabled' => true,
         'keys' => [
          '#private' => $this->getPrivateKey(),
          'public' => $this->getPublicKey(),
         ],
         'user' => 'root',
         'sshHost' => 'sshproxy',
         'remoteHost' => 'mssql',
         'remotePort' => '1433',
         'localPort' => '1235',
        ];

        $config['parameters']['tables'] = [];

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testGetTables(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(6, $result['tables']);

        $expectedData = array (
            0 =>
                array (
                    'name' => 'auto Increment Timestamp',
                    'catalog' => 'test',
                    'schema' => 'dbo',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => '_Weir%d I-D',
                                    'sanitizedName' => 'Weir_d_I_D',
                                    'type' => 'int',
                                    'length' => '10',
                                    'nullable' => false,
                                    'ordinalPosition' => 1,
                                    'primaryKey' => true,
                                    'autoIncrement' => true,
                                    'uniqueKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'Weir%d Na-me',
                                    'sanitizedName' => 'Weir_d_Na_me',
                                    'type' => 'varchar',
                                    'length' => '55',
                                    'nullable' => false,
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'someInteger',
                                    'sanitizedName' => 'someInteger',
                                    'type' => 'int',
                                    'length' => '10',
                                    'nullable' => true,
                                    'ordinalPosition' => 3,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'someDecimal',
                                    'sanitizedName' => 'someDecimal',
                                    'type' => 'decimal',
                                    'length' => '10,2',
                                    'nullable' => true,
                                    'ordinalPosition' => 4,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'type',
                                    'sanitizedName' => 'type',
                                    'type' => 'varchar',
                                    'length' => '55',
                                    'nullable' => true,
                                    'ordinalPosition' => 5,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'smalldatetime',
                                    'sanitizedName' => 'smalldatetime',
                                    'type' => 'smalldatetime',
                                    'nullable' => true,
                                    'ordinalPosition' => 6,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'datetime',
                                    'sanitizedName' => 'datetime',
                                    'type' => 'datetime',
                                    'nullable' => false,
                                    'ordinalPosition' => 7,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'timestamp',
                                    'sanitizedName' => 'timestamp',
                                    'type' => 'timestamp',
                                    'length' => '8',
                                    'nullable' => false,
                                    'ordinalPosition' => 8,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
            1 =>
                array (
                    'name' => 'change Tracking',
                    'catalog' => 'test',
                    'schema' => 'dbo',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'id',
                                    'sanitizedName' => 'id',
                                    'type' => 'int',
                                    'length' => '10',
                                    'nullable' => false,
                                    'ordinalPosition' => 1,
                                    'primaryKey' => true,
                                    'autoIncrement' => true,
                                    'uniqueKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'name',
                                    'sanitizedName' => 'name',
                                    'type' => 'varchar',
                                    'length' => '55',
                                    'nullable' => false,
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'someInteger',
                                    'sanitizedName' => 'someInteger',
                                    'type' => 'int',
                                    'length' => '10',
                                    'nullable' => true,
                                    'ordinalPosition' => 3,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'someDecimal',
                                    'sanitizedName' => 'someDecimal',
                                    'type' => 'decimal',
                                    'length' => '10,2',
                                    'nullable' => true,
                                    'ordinalPosition' => 4,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'type',
                                    'sanitizedName' => 'type',
                                    'type' => 'varchar',
                                    'length' => '55',
                                    'nullable' => true,
                                    'ordinalPosition' => 5,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'smalldatetime',
                                    'sanitizedName' => 'smalldatetime',
                                    'type' => 'smalldatetime',
                                    'nullable' => true,
                                    'ordinalPosition' => 6,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'datetime',
                                    'sanitizedName' => 'datetime',
                                    'type' => 'datetime',
                                    'nullable' => false,
                                    'ordinalPosition' => 7,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'timestamp',
                                    'sanitizedName' => 'timestamp',
                                    'type' => 'timestamp',
                                    'length' => '8',
                                    'nullable' => false,
                                    'ordinalPosition' => 8,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
            2 =>
                array (
                    'name' => 'change Tracking 2',
                    'catalog' => 'test',
                    'schema' => 'dbo',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'id',
                                    'sanitizedName' => 'id',
                                    'type' => 'int',
                                    'length' => '10',
                                    'nullable' => false,
                                    'ordinalPosition' => 1,
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                    'autoIncrement' => true,
                                ),
                            1 =>
                                array (
                                    'name' => 'name',
                                    'sanitizedName' => 'name',
                                    'type' => 'varchar',
                                    'length' => '55',
                                    'nullable' => false,
                                    'ordinalPosition' => 2,
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'someInteger',
                                    'sanitizedName' => 'someInteger',
                                    'type' => 'int',
                                    'length' => '10',
                                    'nullable' => true,
                                    'ordinalPosition' => 3,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
            3 =>
                array (
                    'name' => 'sales',
                    'catalog' => 'test',
                    'schema' => 'dbo',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'usergender',
                                    'sanitizedName' => 'usergender',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 1,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'sanitizedName' => 'usercity',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'sanitizedName' => 'usersentiment',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 3,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'sanitizedName' => 'zipcode',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 4,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'sanitizedName' => 'sku',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 5,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'sanitizedName' => 'createdat',
                                    'type' => 'varchar',
                                    'length' => '64',
                                    'nullable' => false,
                                    'ordinalPosition' => 6,
                                    'primaryKey' => true,
                                    'uniqueKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'sanitizedName' => 'category',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 7,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'sanitizedName' => 'price',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 8,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'sanitizedName' => 'county',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 9,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'sanitizedName' => 'countycode',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 10,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'sanitizedName' => 'userstate',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 11,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'sanitizedName' => 'categorygroup',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 12,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
            4 =>
                array (
                    'name' => 'sales2',
                    'catalog' => 'test',
                    'schema' => 'dbo',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'usergender',
                                    'sanitizedName' => 'usergender',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 1,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'sanitizedName' => 'usercity',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'sanitizedName' => 'usersentiment',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 3,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'sanitizedName' => 'zipcode',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 4,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'sanitizedName' => 'sku',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 5,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'sanitizedName' => 'createdat',
                                    'type' => 'varchar',
                                    'length' => 64,
                                    'nullable' => false,
                                    'ordinalPosition' => 6,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'sanitizedName' => 'category',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 7,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'sanitizedName' => 'price',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 8,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'sanitizedName' => 'county',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 9,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'sanitizedName' => 'countycode',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 10,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'sanitizedName' => 'userstate',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 11,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'sanitizedName' => 'categorygroup',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 12,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
            5 =>
                array (
                    'name' => 'special',
                    'catalog' => 'test',
                    'schema' => 'dbo',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'sanitizedName' => 'col1',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 1,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'sanitizedName' => 'col2',
                                    'type' => 'text',
                                    'nullable' => true,
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                    'uniqueKey' => false,
                                ),
                        ),
                ),
        );

        $this->assertEquals($expectedData, $result['tables']);
    }

    public function testColumnOrdering(): void
    {
        $salesManifestFile = $this->dataDir . '/out/tables/in.c-main.columnscheck.csv.manifest';
        $salesDataFile = $this->dataDir . '/out/tables/in.c-main.columnscheck.csv';
        @unlink($salesDataFile);
        @unlink($salesManifestFile);

        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);

        unset($config['parameters']['tables'][0]['query']);
        $config['parameters']['tables'][0]['table'] = ['tableName' => 'sales', 'schema' => 'dbo'];
        $config['parameters']['tables'][0]['columns'] = ['createdat', 'categorygroup', 'sku', 'zipcode', 'userstate'];
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.columnsCheck';
        $result = $this->createApplication($config)->run();

        $this->assertEquals('success', $result['status']);
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.columnscheck.csv.manifest';
        $outputManifest = json_decode((string) file_get_contents($outputManifestFile), true);
        // check that the manifest has the correct column ordering
        $this->assertEquals($config['parameters']['tables'][0]['columns'], $outputManifest['columns']);
        // check the data
        $expectedData = iterator_to_array(new CsvFile($this->dataDir.'/mssql/columnsOrderCheck.csv'));
        $outputData = iterator_to_array(new CsvFile($this->dataDir.'/out/tables/in.c-main.columnscheck.csv'));
        foreach ($outputData as $rowNum => $line) {
            // assert timestamp
            $this->assertEquals($line[0], $expectedData[$rowNum][0]);
            $this->assertEquals($line[1], $expectedData[$rowNum][1]);
            $this->assertEquals($line[2], $expectedData[$rowNum][2]);
        }
    }

    public function testXMLtoNVarchar(): void
    {
        $this->dropTable('XML_TEST');
        $this->pdo->exec('CREATE TABLE [XML_TEST] ([ID] INT NOT NULL, [XML_COL] XML NULL);');
        $this->pdo->exec(
            "INSERT INTO [XML_TEST] VALUES (1, '<test>some test xml </test>'), (2, null), (3, '<test>some test xml </test>')"
        );
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        unset($config['parameters']['tables'][0]['query']);
        $config['parameters']['tables'][0]['table'] = ['tableName' => 'XML_TEST', 'schema' => 'dbo'];
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.xml_test';

        $result = $this->createApplication($config)->run();

        $this->assertEquals('success', $result['status']);

        $this->dropTable('XML_TEST');
    }

    public function testStripNulls(): void
    {
        $this->dropTable('NULL_TEST');
        $this->pdo->exec("CREATE TABLE [NULL_TEST] ([ID] VARCHAR(5) NULL, [NULL_COL] NVARCHAR(10) DEFAULT '', [col2] VARCHAR(55));");
        $this->pdo->exec(
            "INSERT INTO [NULL_TEST] VALUES
            ('', '', 'test with ' + CHAR(0) + ' inside'),
            ('', '', ''),
            ('3', '', 'test')"
        );
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        unset($config['parameters']['tables'][0]['table']);
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM [NULL_TEST]';
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.null_test';

        $result = $this->createApplication($config)->run();

        $outputData = iterator_to_array(new CsvFile($this->dataDir . '/out/tables/in.c-main.null_test.csv'));

        $this->assertStringNotContainsString(chr(0), $outputData[0][0]);
        $this->assertStringNotContainsString(chr(0), $outputData[0][1]);
        $this->assertEquals('test with ' . chr(0) . ' inside', $outputData[0][2]);
        $this->assertStringNotContainsString(chr(0), $outputData[1][0]);
        $this->assertStringNotContainsString(chr(0), $outputData[1][1]);
        $this->assertStringNotContainsString(chr(0), $outputData[1][2]);
        $this->assertStringNotContainsString(chr(0), $outputData[2][1]);
        $this->assertEquals('success', $result['status']);

        $this->dropTable('NULL_TEST');
    }

    public function testMultipleSelectStatements(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        unset($config['parameters']['tables'][0]['table']);
        $config['parameters']['tables'][0]['query'] = "SELECT usergender INTO #temptable FROM sales WHERE usergender LIKE 'undefined';  SELECT * FRoM sales WHERE usergender IN (SELECT * FROM #temptable);";
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.multipleselect_test';

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Failed to retrieve results: SQLSTATE[IMSSP]: The active result for the query contains no fields. Code:IMSSP');
        $this->createApplication($config)->run();
    }

    /**
     * @dataProvider configProvider
     */
    public function testManifestMetadata(array $config): void
    {
        $isConfigRow = !isset($config['parameters']['tables']);

        $tableParams = ($isConfigRow) ? $config['parameters'] : $config['parameters']['tables'][0];
        unset($tableParams['query']);
        $tableParams['name'] = 'sales2';
        $tableParams['outputTable'] = 'in.c-main.sales2';
        $tableParams['primaryKey'] = ['createdat'];
        $tableParams['table'] = [
            'tableName' => 'sales2',
            'schema' => 'dbo',
        ];
        if ($isConfigRow) {
            $config['parameters'] = $tableParams;
        } else {
            $config['parameters']['tables'][0] = $tableParams;
            unset($config['parameters']['tables'][1]);
            unset($config['parameters']['tables'][2]);
            unset($config['parameters']['tables'][3]);
        }

        $result = $this->createApplication($config)->run();

        $importedTable = ($isConfigRow) ? $result['imported']['outputTable'] : $result['imported'][0]['outputTable'];

        $outputManifest = json_decode(
            (string) file_get_contents($this->dataDir . '/out/tables/' . $importedTable . '.csv.manifest'),
            true
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(12, $outputManifest['column_metadata']);

        $expectedColumnMetadata = [
            'usergender' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'text',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '2147483647',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'usergender',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'usergender',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 1,
                ],
            ],
            'usercity' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'text',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '2147483647',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'usercity',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'usercity',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 2,
                ],
            ],
            'usersentiment' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'text',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '2147483647',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'usersentiment',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'usersentiment',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 3,
                ],
            ],
            'zipcode' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'text',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '2147483647',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'zipcode',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'zipcode',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 4,
                ],
            ],
            'sku' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'text',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '2147483647',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'sku',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'sku',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 5,
                ],
            ],
            'createdat' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'varchar',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '64',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'createdat',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'createdat',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 6,
                ],
                [
                    'key' => 'KBC.foreignKey',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.foreignKeyName',
                    'value' => 'FK_sales_sales2',
                ],
                [
                    'key' => 'KBC.foreignKeyRefSchema',
                    'value' => 'dbo',
                ],
                [
                    'key' => 'KBC.foreignKeyRefTable',
                    'value' => 'sales',
                ],
                [
                    'key' => 'KBC.foreignKeyRefColumn',
                    'value' => 'createdat',
                ],
            ],
            'category' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'text',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '2147483647',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'category',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'category',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 7,
                ],
            ],
            'price' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'text',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '2147483647',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'price',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'price',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 8,
                ],
            ],
            'county' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'text',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '2147483647',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'county',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'county',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 9,
                ],
            ],
            'countycode' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'text',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '2147483647',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'countycode',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'countycode',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 10,
                ],
            ],
            'userstate' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'text',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '2147483647',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'userstate',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'userstate',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 11,
                ],
            ],
            'categorygroup' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'text',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '2147483647',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'categorygroup',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'categorygroup',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 12,
                ],
            ],
        ];

        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }
}
