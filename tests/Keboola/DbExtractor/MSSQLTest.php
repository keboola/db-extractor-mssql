<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;

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

    public function testRunWithSSH(): void
    {
        $config = $this->getConfig('mssql', 'json');
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
            'localPort' => '1234',
        ];
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
                                        'value' => '(getdate())',
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
                                    'default' => '(getdate())',
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
}
