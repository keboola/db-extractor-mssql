<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;


use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;
use Keboola\DbExtractor\MSSQLApplication;

class MSSQLTest extends AbstractMSSQLTest
{
    public function testCredentials(): void
    {
        $config = $this->getConfig('mssql');
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

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
        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        try {
            $app->run();
            $this->fail('Must raise exception');
        } catch (UserException $e) {
            $this->assertContains('Cannot open database "nonExistentDb" requested by the login.', $e->getMessage());
        }
    }

    public function testRunWithoutTables(): void
    {
        $config = $this->getConfig('mssql');

        unset($config['parameters']['tables']);

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

        $this->checkResult($result);
    }

    private function checkResult(array $result): void
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
        $manifest = json_decode(file_get_contents($salesManifestFile), true);
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
        $manifest = json_decode(file_get_contents($tableColumnsManifest), true);
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
                                'key' => 'KBC.catalog',
                                'value' => 'test',
                            ),
                        2 =>
                            array (
                                'key' => 'KBC.schema',
                                'value' => 'dbo',
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
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 1,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
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
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 2,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
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
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 3,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
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
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => 4,
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
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
        $manifest = json_decode(file_get_contents($weirdManifest), true);
        // assert the timestamp column has the correct date format
        $outputData = iterator_to_array(
            new CsvFile($this->dataDir . '/out/tables/' . $result['imported'][2]['outputTable'] . '.csv')
        );
        $firstTimestamp = $outputData[0][3];
        // there should be no decimal separator present (it should be cast to smalldatetime which does not include ms)
        $this->assertEquals("2018-08-14 10:43:18", $firstTimestamp);
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
                                'key' => 'KBC.catalog',
                                'value' => 'test',
                            ),
                        2 =>
                            array (
                                'key' => 'KBC.schema',
                                'value' => 'dbo',
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
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => '1',
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => true,
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.primaryKeyName',
                                        'value' => 'PK_AUTOINC',
                                    ),
                                9 =>
                                    array (
                                        'key' => 'KBC.checkConstraint',
                                        'value' => 'CHK_ID_CONTSTRAINT',
                                    ),
                                10 =>
                                    array (
                                        'key' => 'KBC.checkClause',
                                        'value' => '([_Weir%d I-D]>(0) AND [_Weir%d I-D]<(20))',
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
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => '2',
                                    ),
                                8 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                                9 =>
                                    array (
                                        'key' => 'KBC.uniqueKey',
                                        'value' => true,
                                    ),
                                10 =>
                                    array (
                                        'key' => 'KBC.uniqueKeyName',
                                        'value' => 'UNI_KEY_1',
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
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => '3',
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
                                        'key' => 'KBC.uniqueKeyName',
                                        'value' => 'UNI_KEY_1',
                                    ),
                            ),
                        'timestamp' =>
                            array (
                                0 =>
                                    array (
                                        'key' => 'KBC.datatype.type',
                                        'value' => 'datetime',
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
                                        'value' => '(\'2018-08-14 10:43:18\')',
                                    ),
                                4 =>
                                    array (
                                        'key' => 'KBC.sourceName',
                                        'value' => 'timestamp',
                                    ),
                                5 =>
                                    array (
                                        'key' => 'KBC.sanitizedName',
                                        'value' => 'timestamp',
                                    ),
                                6 =>
                                    array (
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => '4',
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
                                    ),
                            ),
                    ),
                'columns' =>
                    array (
                        0 => 'Weir_d_I_D',
                        1 => 'Weir_d_Na_me',
                        2 => 'type',
                        3 => 'timestamp',
                    ),
            ),
            $manifest
        );

        $specialManifest = $this->dataDir . '/out/tables/' . $result['imported'][3]['outputTable'] . '.csv.manifest';
        $manifest = json_decode(file_get_contents($specialManifest), true);
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
                                'key' => 'KBC.catalog',
                                'value' => 'test',
                            ),
                        2 =>
                            array (
                                'key' => 'KBC.schema',
                                'value' => 'dbo',
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
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => '1',
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
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
                                        'key' => 'KBC.ordinalPosition',
                                        'value' => '2',
                                    ),
                                7 =>
                                    array (
                                        'key' => 'KBC.primaryKey',
                                        'value' => false,
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
          '#private' => $this->getPrivateKey('mssql'),
          'public' => $this->getEnv('mssql', 'DB_SSH_KEY_PUBLIC'),
         ],
         'user' => 'root',
         'sshHost' => 'sshproxy',
         'remoteHost' => 'mssql',
         'remotePort' => '1433',
         'localPort' => '1235',
        ];

        unset($config['parameters']['tables']);

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
        $this->assertCount(4, $result['tables']);
        $expectedData = array (
            0 =>
                array (
                    'name' => 'auto Increment Timestamp',
                    'catalog' => 'test',
                    'schema' => 'dbo',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            2 =>
                                array (
                                    'name' => 'type',
                                    'sanitizedName' => 'type',
                                    'type' => 'varchar',
                                    'length' => 55,
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 3,
                                    'primaryKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'timestamp',
                                    'sanitizedName' => 'timestamp',
                                    'type' => 'datetime',
                                    'length' => null,
                                    'nullable' => true,
                                    'default' => '(\'2018-08-14 10:43:18\')',
                                    'ordinalPosition' => 4,
                                    'primaryKey' => false,
                                ),
                            0 =>
                                array (
                                    'name' => '_Weir%d I-D',
                                    'sanitizedName' => 'Weir_d_I_D',
                                    'type' => 'int',
                                    'length' => 10,
                                    'nullable' => false,
                                    'default' => null,
                                    'ordinalPosition' => 1,
                                    'primaryKey' => true,
                                    'primaryKeyName' => 'PK_AUTOINC',
                                ),
                            1 =>
                                array (
                                    'name' => 'Weir%d Na-me',
                                    'sanitizedName' => 'Weir_d_Na_me',
                                    'type' => 'varchar',
                                    'length' => 55,
                                    'nullable' => false,
                                    'default' => '(\'mario\')',
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            1 =>
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
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 1,
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'sanitizedName' => 'usercity',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'sanitizedName' => 'usersentiment',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 3,
                                    'primaryKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'sanitizedName' => 'zipcode',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 4,
                                    'primaryKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'sanitizedName' => 'sku',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 5,
                                    'primaryKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'sanitizedName' => 'createdat',
                                    'type' => 'varchar',
                                    'length' => 64,
                                    'nullable' => false,
                                    'default' => null,
                                    'ordinalPosition' => 6,
                                    'primaryKey' => true,
                                    'primaryKeyName' => 'PK_sales',
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'sanitizedName' => 'category',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 7,
                                    'primaryKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'sanitizedName' => 'price',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 8,
                                    'primaryKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'sanitizedName' => 'county',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 9,
                                    'primaryKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'sanitizedName' => 'countycode',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 10,
                                    'primaryKey' => false,
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'sanitizedName' => 'userstate',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 11,
                                    'primaryKey' => false,
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'sanitizedName' => 'categorygroup',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 12,
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            2 =>
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
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 1,
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'sanitizedName' => 'usercity',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 2,
                                    'primaryKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'sanitizedName' => 'usersentiment',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 3,
                                    'primaryKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'sanitizedName' => 'zipcode',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 4,
                                    'primaryKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'sanitizedName' => 'sku',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 5,
                                    'primaryKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'sanitizedName' => 'createdat',
                                    'type' => 'varchar',
                                    'length' => 64,
                                    'nullable' => false,
                                    'default' => null,
                                    'ordinalPosition' => 6,
                                    'primaryKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'sanitizedName' => 'category',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 7,
                                    'primaryKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'sanitizedName' => 'price',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 8,
                                    'primaryKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'sanitizedName' => 'county',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 9,
                                    'primaryKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'sanitizedName' => 'countycode',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 10,
                                    'primaryKey' => false,
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'sanitizedName' => 'userstate',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 11,
                                    'primaryKey' => false,
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'sanitizedName' => 'categorygroup',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => 12,
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            3 =>
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
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '1',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'sanitizedName' => 'col2',
                                    'type' => 'text',
                                    'length' => '2147483647',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
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
        $config['parameters']['tables'][0]['columns'] = ["createdat", "categorygroup", "sku", "zipcode", "userstate"];
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.columnsCheck';
        $result = $this->createApplication($config)->run();

        $this->assertEquals('success', $result['status']);
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.columnscheck.csv.manifest';
        $outputManifest = json_decode(file_get_contents($outputManifestFile), true);
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
        $this->pdo->exec("IF OBJECT_ID('dbo.XML_TEST', 'U') IS NOT NULL DROP TABLE dbo.XML_TEST");
        $this->pdo->exec("CREATE TABLE [XML_TEST] ([ID] INT NOT NULL, [XML_COL] XML NULL);");
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
        
        $this->pdo->exec("IF OBJECT_ID('dbo.XML_TEST', 'U') IS NOT NULL DROP TABLE dbo.XML_TEST");
    }

    public function testStripNulls(): void
    {
        $this->pdo->exec("IF OBJECT_ID('dbo.NULL_TEST', 'U') IS NOT NULL DROP TABLE dbo.NULL_TEST");
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
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM [NULL_TEST]";
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.null_test';

        $result = $this->createApplication($config)->run();

        $outputData = iterator_to_array(new CsvFile($this->dataDir . '/out/tables/in.c-main.null_test.csv'));

        $this->assertNotContains(chr(0), $outputData[0][0]);
        $this->assertNotContains(chr(0), $outputData[0][1]);
        $this->assertEquals("test with " . chr(0) . " inside", $outputData[0][2]);
        $this->assertNotContains(chr(0), $outputData[1][0]);
        $this->assertNotContains(chr(0), $outputData[1][1]);
        $this->assertNotContains(chr(0), $outputData[1][2]);
        $this->assertNotContains(chr(0), $outputData[2][1]);
        $this->assertEquals('success', $result['status']);

        $this->pdo->exec("IF OBJECT_ID('dbo.XML_TEST', 'U') IS NOT NULL DROP TABLE dbo.XML_TEST");
    }

    public function testThousandsOfTablesGetTables(): void
    {
        $this->markTestSkipped("No need to run this test every time.");
        $numberOfTables = 5000;

        $createTableScript = '';
        $dropTableScript = '';
        for ($i = 0; $i < $numberOfTables; $i++) {
            $createTableScript .= "CREATE TABLE tmp_" . $i . " (id int identity primary key, kafka VARCHAR(255))\n";
            $dropTableScript .= "DROP TABLE tmp_" . $i . "\n";
        }

        $dropTableScript = "Begin\n" . $dropTableScript . "End\n";
        $createTableScript = "Begin\n" . $createTableScript . "End\n";

        // cleanup
        try {
            $this->pdo->exec($dropTableScript);
        } catch (\Throwable $e) {
            // one or more tables didn't exist
        }
        // wait for the drop script to finish
        sleep(5);

        $this->pdo->exec($createTableScript);
        // wait for the create script to finish
        sleep(5);

        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = $this->createApplication($config);

        $startTime = time();
        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $runTime = time() - $startTime;

        echo "\n" . $numberOfTables . " tables were fetched in " . $runTime . " seconds.\n";
        // cleanup
        try {
            $this->pdo->exec($dropTableScript);
        } catch (\Throwable $e) {
            // one or more table didn't exist
        }
    }

    public function testLargeTableRun(): void
    {
        $this->markTestSkipped("No need to run this test every time.");

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

        try {
            $this->pdo->exec("DROP TABLE largetest");
        } catch (\Throwable $e) {
            // table didn't exist
        }
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

        try {
            $this->pdo->exec("DROP TABLE largetest");
        } catch (\Throwable $e) {
            // table didn't exist
        }
    }
}
