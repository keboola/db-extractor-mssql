<?php
/**
 * @package ex-db-mssql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Yaml\Yaml;
use Nette\Utils;

class MSSQLTest extends AbstractMSSQLTest
{
    public function testCredentials()
    {
        $config = $this->getConfig('mssql');
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunWithoutTables()
    {
        $config = $this->getConfig('mssql');

        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRun()
    {
        $config = $this->getConfig('mssql');

        $app = $this->createApplication($config);

        $csv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');

        $result = $app->run();

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
        $this->assertFileEquals((string) $csv1, $outputCsvFile);
    }

    public function testCredentialsWithSSH()
    {
        $config = $this->getConfig('mssql');
        $config['action'] = 'testConnection';

        $config['parameters']['db']['ssh'] = [
         'enabled' => true,
         'keys' => [
          '#private' => $this->getPrivateKey('mssql'),
          'public' => $this->getEnv('mssql', 'DB_SSH_KEY_PUBLIC')
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

    public function testRunWithSSH()
    {
        $config = $this->getConfig('mssql');
        $config['parameters']['db']['ssh'] = [
         'enabled' => true,
         'keys' => [
          '#private' => $this->getPrivateKey('mssql'),
          'public' => $this->getEnv('mssql', 'DB_SSH_KEY_PUBLIC')
         ],
         'user' => 'root',
         'sshHost' => 'sshproxy',
         'remoteHost' => 'mssql',
         'remotePort' => '1433',
         'localPort' => '1234',
        ];

        $app = $this->createApplication($config);

        $csv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');

        $result = $app->run();

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
        $this->assertFileEquals((string) $csv1, $outputCsvFile);
    }

    public function testGetTables()
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';

        $app = new Application($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(3, $result['tables']);
        $expectedData = array (
            0 =>
                array (
                    'name' => 'autoIncrement',
                    'catalog' => 'test',
                    'schema' => 'dbo',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            2 =>
                                array (
                                    'name' => 'Type',
                                    'type' => 'varchar',
                                    'length' => '55',
                                    'nullable' => false,
                                    'default' => null,
                                    'ordinalPosition' => '3',
                                    'primaryKey' => false,
                                ),
                            0 =>
                                array (
                                    'name' => 'ID',
                                    'type' => 'int',
                                    'length' => '10',
                                    'nullable' => false,
                                    'default' => null,
                                    'ordinalPosition' => '1',
                                    'primaryKey' => true,
                                    'primaryKeyName' => 'PK_AUTOINC',
                                ),
                            1 =>
                                array (
                                    'name' => 'Name',
                                    'type' => 'varchar',
                                    'length' => '55',
                                    'nullable' => false,
                                    'default' => '(\'mario\')',
                                    'ordinalPosition' => '2',
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
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '1',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '3',
                                    'primaryKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '4',
                                    'primaryKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '5',
                                    'primaryKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'type' => 'varchar',
                                    'length' => '64',
                                    'nullable' => false,
                                    'default' => null,
                                    'ordinalPosition' => '6',
                                    'primaryKey' => true,
                                    'primaryKeyName' => 'PK_sales',
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '7',
                                    'primaryKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '8',
                                    'primaryKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '9',
                                    'primaryKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '10',
                                    'primaryKey' => false,
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '11',
                                    'primaryKey' => false,
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '12',
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
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '1',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '2',
                                    'primaryKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '3',
                                    'primaryKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '4',
                                    'primaryKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '5',
                                    'primaryKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'type' => 'varchar',
                                    'length' => '64',
                                    'nullable' => false,
                                    'default' => null,
                                    'ordinalPosition' => '6',
                                    'primaryKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '7',
                                    'primaryKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '8',
                                    'primaryKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '9',
                                    'primaryKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '10',
                                    'primaryKey' => false,
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '11',
                                    'primaryKey' => false,
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'type' => 'varchar',
                                    'length' => '255',
                                    'nullable' => true,
                                    'default' => null,
                                    'ordinalPosition' => '12',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
        );

        $this->assertEquals($expectedData, $result['tables']);
    }

    public function testManifestMetadata()
    {
        $config = $this->getConfig();

        // use just 1 table
        unset($config['parameters']['tables']);

        $config['parameters']['tables'] = [
            0 => [
                "id" => 1,
                "name" => "plumbers",
                "table" => [
                    "tableName" => "autoIncrement",
                    "schema" => "dbo"
                ],
                "outputTable" => "in.c-main.autoIncrement",
                "incremental" => false,
                "primaryKey" => ["ID"],
                "enabled" => true
            ]
        ];

        $app = new Application($config);

        $result = $app->run();

        $sanitizedOutputTable = Utils\Strings::webalize($result['imported'][0], '._');
        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/' . $sanitizedOutputTable . '.csv.manifest')
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $expectedTableMetadata = array (
            0 =>
                array (
                    'key' => 'KBC.name',
                    'value' => 'autoIncrement',
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
        );
        $this->assertEquals($expectedTableMetadata, $outputManifest['metadata']);

        $this->assertArrayHasKey('column_metadata', $outputManifest);

        $expectedColumnMetadata = array (
            'ID' =>
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
                            'key' => 'KBC.ordinalPosition',
                            'value' => '1',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => true,
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.primaryKeyName',
                            'value' => 'PK_AUTOINC',
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.checkConstraint',
                            'value' => 'CHK_ID_CONTSTRAINT',
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.checkClause',
                            'value' => '([ID] > 0 and [ID] < 20)',
                        ),
                ),
            'Name' =>
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
                            'key' => 'KBC.ordinalPosition',
                            'value' => '2',
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
                            'key' => 'KBC.uniqueKeyName',
                            'value' => 'UNI_KEY_1',
                        ),
                ),
            'Type' =>
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
                            'key' => 'KBC.ordinalPosition',
                            'value' => '3',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => true,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.uniqueKeyName',
                            'value' => 'UNI_KEY_1',
                        ),
                ),
        );

        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }
}
