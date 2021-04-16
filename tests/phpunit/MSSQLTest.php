<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Adapter\Exception\UserRetriedException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use Keboola\DbExtractor\Metadata\MssqlManifestSerializer;
use Keboola\DbExtractor\Metadata\MssqlMetadataProvider;
use Keboola\DbExtractor\MSSQLApplication;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use Keboola\DbExtractor\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SalesTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SimpleTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SpecialTableTrait;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use \PDO;

class MSSQLTest extends TestCase
{
    use ConfigTrait;
    use SalesTableTrait;
    use SpecialTableTrait;
    use AutoIncrementTableTrait;
    use SimpleTableTrait;
    use RemoveAllTablesTrait;
    use CloseSshTunnelsTrait;

    private string $dataDir = __DIR__ . '/data';

    protected PDO $connection;

    protected function setUp(): void
    {
        $this->connection = PdoTestConnection::createConnection();
        $this->removeAllTables();
        $this->closeSshTunnels();
    }

    public function testCredentialsWrongDb(): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['database'] = 'nonExistentDb';
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];

        $app = $this->createApplication($config);
        try {
            $app->run();
            $this->fail('Must raise exception');
        } catch (UserException $e) {
            $this->assertStringContainsString(
                'Cannot open database "nonExistentDb" requested by the login.',
                $e->getMessage()
            );
        }
    }

    public function testRunWithoutTables(): void
    {
        $config = $this->getConfig();

        $config['parameters']['tables'] = [];

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunNoRows(): void
    {
        $this->createSalesTable();
        $this->generateSalesRows();

        $salesManifestFile = $this->dataDir . '/out/tables/in.c-main.sales.csv.manifest';
        $salesDataFile = $this->dataDir . '/out/tables/in.c-main.sales.csv';
        @unlink($salesDataFile);
        @unlink($salesManifestFile);

        $config = $this->getConfig();
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);

        $config['parameters']['tables'][0]['query'] = "SELECT * FROM sales WHERE usergender LIKE 'undefined'";

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);

        $this->assertFileExists($salesManifestFile);
        $this->assertFileExists($salesDataFile);
    }

    /**
     * @dataProvider configProvider
     */
    public function testRunConfig(array $config): void
    {
        $this->createSalesTable();
        $this->createAITable();
        $this->createSpecialTable();
        $this->generateSalesRows();
        $this->generateAIRows();
        $this->generateSpecialRows();
        $this->addAIConstraint();

        $result = $this->createApplication($config)->run();
        if (array_key_exists('tables', $config['parameters'])) {
            $this->checkTablesResult($result);
        } else {
            $this->checkRowResult($result);
        }
    }

    public function testCredentialsWithSSH(): void
    {
        $config = $this->getConfig();
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

    public function testXMLtoNVarchar(): void
    {
        $this->connection->exec('CREATE TABLE [XML_TEST] ([ID] INT NOT NULL, [XML_COL] XML NULL);');
        $this->connection->exec(
            'INSERT INTO [XML_TEST] ' .
            "VALUES (1, '<test>some test xml </test>'), (2, null), (3, '<test>some test xml </test>')"
        );
        $config = $this->getConfig();
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        unset($config['parameters']['tables'][0]['query']);
        $config['parameters']['tables'][0]['table'] = ['tableName' => 'XML_TEST', 'schema' => 'dbo'];
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.xml_test';

        $result = $this->createApplication($config)->run();

        $this->assertEquals('success', $result['status']);
    }

    public function testStripNulls(): void
    {
        $this->connection->exec(
            'CREATE TABLE [NULL_TEST] ' .
            "([ID] VARCHAR(5) NULL, [NULL_COL] NVARCHAR(10) DEFAULT '', [col2] VARCHAR(55));"
        );
        $this->connection->exec(
            "INSERT INTO [NULL_TEST] VALUES
            ('', '', 'test with ' + CHAR(0) + ' inside'),
            ('', '', ''),
            ('3', '', 'test')"
        );
        $config = $this->getConfig();
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        unset($config['parameters']['tables'][0]['table']);
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM [NULL_TEST]';
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.null_test';

        $result = $this->createApplication($config)->run();

        $outputData = iterator_to_array(new CsvReader($this->dataDir . '/out/tables/in.c-main.null_test.csv'));

        $this->assertStringNotContainsString(chr(0), $outputData[0][0]);
        $this->assertStringNotContainsString(chr(0), $outputData[0][1]);
        $this->assertEquals('test with ' . chr(0) . ' inside', $outputData[0][2]);
        $this->assertStringNotContainsString(chr(0), $outputData[1][0]);
        $this->assertStringNotContainsString(chr(0), $outputData[1][1]);
        $this->assertStringNotContainsString(chr(0), $outputData[1][2]);
        $this->assertStringNotContainsString(chr(0), $outputData[2][1]);
        $this->assertEquals('success', $result['status']);
    }

    public function testMultipleSelectStatements(): void
    {
        $this->createSalesTable();
        $this->generateSalesRows();

        $config = $this->getConfig();
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        unset($config['parameters']['tables'][0]['table']);
        $config['parameters']['tables'][0]['query'] =
            'SELECT usergender INTO #temptable ' .
            "FROM sales WHERE usergender LIKE 'undefined';  " .
            'SELECT * FRoM sales WHERE usergender IN (SELECT * FROM #temptable);';
        $config['parameters']['tables'][0]['name'] = 'multipleselect_test';
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.multipleselect_test';

        $this->expectException(UserRetriedException::class);
        $this->expectExceptionMessage(
            '[in.c-main.multipleselect_test]: DB query failed: SQLSTATE[IMSSP]: ' .
            'The active result for the query contains no fields. Tried 5 times.'
        );
        $this->createApplication($config)->run();
    }

    /**
     * @dataProvider configProvider
     */
    public function testManifestMetadata(array $config): void
    {
        $this->createSalesTable();
        $this->createSalesTable('sales2');
        $this->createAITable();
        $this->createSpecialTable();
        $this->generateSalesRows();
        $this->generateSalesRows('sales2');
        $this->generateAIRows();
        $this->generateSpecialRows();
        $this->addAIConstraint();
        $this->addSalesConstraint('sales', ['createdat']);
        $this->addSalesConstraint('sales2');

        // second sales table with foreign key
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

    public function testMultipleConstraintsGetTables(): void
    {
        $this->createSimpleTable();
        $this->addSimpleConstraint();

        // Column with multiple constraints must be present in metadata only once.
        $this->connection->exec('ALTER TABLE [simple] ADD CONSTRAINT c1 UNIQUE ([name]);');
        $this->connection->exec('ALTER TABLE [simple] ADD CONSTRAINT c2 CHECK (LEN([name]) > 0);');
        $this->connection->exec('ALTER TABLE [simple] ADD CONSTRAINT c3 CHECK (LEN([name]) > 1);');
        $this->connection->exec('ALTER TABLE [simple] ADD CONSTRAINT c4 CHECK (LEN([name]) > 2);');

        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $result = $this->createApplication($config)->run();
        $tables = array_values(array_filter($result['tables'], fn(array $table) => $table['name'] === 'simple'));
        $this->assertSame(
            [
                [
                    'name' => 'simple',
                    'schema' => 'dbo',
                    'columns' =>
                        [
                            [
                                'name' => 'id',
                                'type' => 'int',
                                'primaryKey' => true,
                            ],
                            [
                                'name' => 'name',
                                'type' => 'varchar',
                                'primaryKey' => false,
                            ],
                        ],
                ],
            ],
            $tables
        );
    }

    public function testMultipleConstraintsManifest(): void
    {
        $this->createSimpleTable();
        $this->addSimpleConstraint();

        // Column with multiple constraints must be present in metadata only once.
        $this->connection->exec('ALTER TABLE [simple] ADD CONSTRAINT c1 UNIQUE ([name]);');
        $this->connection->exec('ALTER TABLE [simple] ADD CONSTRAINT c2 CHECK (LEN([name]) > 0);');
        $this->connection->exec('ALTER TABLE [simple] ADD CONSTRAINT c3 CHECK (LEN([name]) > 1);');
        $this->connection->exec('ALTER TABLE [simple] ADD CONSTRAINT c4 CHECK (LEN([name]) > 2);');

        $dbConfig = DatabaseConfig::fromArray($this->getConfig()['parameters']['db']);
        $conn = new MSSQLPdoConnection(new NullLogger(), $dbConfig);
        $metadataProvider = new MssqlMetadataProvider($conn);
        $serializer = new MssqlManifestSerializer();

        $table = new InputTable('simple', 'dbo');
        $columns = $metadataProvider->getTable($table)->getColumns();

        $this->assertSame(['id', 'name'], $columns->getNames());
        $this->assertSame([
            [
                'key' => 'KBC.datatype.type',
                'value' => 'int',
            ],
            [
                'key' => 'KBC.datatype.nullable',
                'value' => false,
            ],
            [
                'key' => 'KBC.datatype.basetype',
                'value' => 'INTEGER',
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '10',
            ],
            [
                'key' => 'KBC.sourceName',
                'value' => 'id',
            ],
            [
                'key' => 'KBC.sanitizedName',
                'value' => 'id',
            ],
            [
                'key' => 'KBC.primaryKey',
                'value' => true,
            ],
            [
                'key' => 'KBC.uniqueKey',
                'value' => false,
            ],
            [
                'key' => 'KBC.ordinalPosition',
                'value' => 1,
            ],
        ], $serializer->serializeColumn($columns->getByName('id')));
        $this->assertSame([
            [
                'key' => 'KBC.datatype.type',
                'value' => 'varchar',
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
                'value' => '100',
            ],
            [
                'key' => 'KBC.sourceName',
                'value' => 'name',
            ],
            [
                'key' => 'KBC.sanitizedName',
                'value' => 'name',
            ],
            [
                'key' => 'KBC.primaryKey',
                'value' => false,
            ],
            [
                'key' => 'KBC.uniqueKey',
                'value' => true,
            ],
            [
                'key' => 'KBC.ordinalPosition',
                'value' => 2,
            ],
        ], $serializer->serializeColumn($columns->getByName('name')));
    }

    public function configProvider(): array
    {
        return [
            [
                $this->getConfig(),
            ],
            [
                $this->getRowConfig(),
            ],
        ];
    }

    private function createApplication(array $config, array $state = []): MSSQLApplication
    {
        $logger = new Logger('ex-db-mssql-tests');
        $app = new MSSQLApplication($config, $logger, $state, $this->dataDir);
        return $app;
    }

    private function getPrivateKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa');
    }

    private function getPublicKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa.pub');
    }

    private function checkRowResult(array $result): void
    {
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.special',
                'rows' => 7,
            ],
            $result['imported']
        );

        $specialManifest = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv.manifest';
        $manifest = json_decode((string) file_get_contents($specialManifest), true);
        $this->assertEquals(
            [
                'destination' => 'in.c-main.special',
                'incremental' => false,
                'metadata' =>
                    [
                        [
                            'key' => 'KBC.name',
                            'value' => 'special',
                        ],
                        [
                            'key' => 'KBC.sanitizedName',
                            'value' => 'special',
                        ],
                        [
                            'key' => 'KBC.schema',
                            'value' => 'dbo',
                        ],
                        [
                            'key' => 'KBC.catalog',
                            'value' => 'test',
                        ],
                        [
                            'key' => 'KBC.type',
                            'value' => 'BASE TABLE',
                        ],
                    ],
                'column_metadata' =>
                    [
                        'col1' =>
                            [
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
                                    'value' => 'col1',
                                ],
                                [
                                    'key' => 'KBC.sanitizedName',
                                    'value' => 'col1',
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
                                    'value' => '1',
                                ],
                            ],
                        'col2' =>
                            [
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
                                    'value' => 'col2',
                                ],
                                [
                                    'key' => 'KBC.sanitizedName',
                                    'value' => 'col2',
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
                                    'value' => '2',
                                ],
                            ],
                    ],
                'columns' =>
                    [
                        'col1',
                        'col2',
                    ],
            ],
            $manifest
        );
    }

    private function checkTablesResult(array $result): void
    {
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                [
                    'outputTable' => 'in.c-main.sales',
                    'rows' => 100,
                ],
                [
                    'outputTable' => 'in.c-main.tablecolumns',
                    'rows' => 100,
                ],
                [
                    'outputTable' => 'in.c-main.auto-increment-timestamp',
                    'rows' => 6,
                ],
                [
                    'outputTable' => 'in.c-main.special',
                    'rows' => 7,
                ],
            ],
            $result['imported']
        );

        $salesManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';
        $manifest = json_decode((string) file_get_contents($salesManifestFile), true);
        $this->assertEquals(
            [
                'destination' => 'in.c-main.sales',
                'incremental' => false,
                'columns' =>
                    [
                        'usergender',
                        'usercity',
                        'usersentiment',
                        'zipcode',
                        'sku',
                        'createdat',
                        'category',
                        'price',
                        'county',
                        'countycode',
                        'userstate',
                        'categorygroup',
                    ],
            ],
            $manifest
        );

        $tableColumnsManifest =
            $this->dataDir . '/out/tables/' . $result['imported'][1]['outputTable'] . '.csv.manifest';
        $manifest = json_decode((string) file_get_contents($tableColumnsManifest), true);
        $this->assertEquals(
            [
                'destination' => 'in.c-main.tablecolumns',
                'incremental' => false,
                'metadata' =>
                    [
                        [
                            'key' => 'KBC.name',
                            'value' => 'sales',
                        ],
                        [
                            'key' => 'KBC.sanitizedName',
                            'value' => 'sales',
                        ],
                        [
                            'key' => 'KBC.schema',
                            'value' => 'dbo',
                        ],
                        [
                            'key' => 'KBC.catalog',
                            'value' => 'test',
                        ],
                        [
                            'key' => 'KBC.type',
                            'value' => 'BASE TABLE',
                        ],
                    ],
                'column_metadata' =>
                    [
                        'usergender' =>
                            [
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
                        'usercity' =>
                            [
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
                        'usersentiment' =>
                            [
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
                        'zipcode' =>
                            [
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
                    ],
                'columns' =>
                    [
                        'usergender',
                        'usercity',
                        'usersentiment',
                        'zipcode',
                    ],
            ],
            $manifest
        );

        $weirdManifest = $this->dataDir . '/out/tables/' . $result['imported'][2]['outputTable'] . '.csv.manifest';
        $manifest = json_decode((string) file_get_contents($weirdManifest), true);
        // assert the timestamp column has the correct date format
        $outputData = iterator_to_array(
            new CsvReader($this->dataDir . '/out/tables/' . $result['imported'][2]['outputTable'] . '.csv')
        );
        $this->assertEquals(1, (int) $outputData[0][2]);
        $this->assertEquals('1.10', $outputData[0][3]);
        $firstTimestamp = $outputData[0][5];
        // there should be no decimal separator present (it should be cast to datetime2(0) which does not include ms)
        $this->assertEquals(1, preg_match('/^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/', $firstTimestamp));
        $this->assertEquals(
            [
                'destination' => 'in.c-main.auto-increment-timestamp',
                'incremental' => false,
                'primary_key' =>
                    [
                        'Weir_d_I_D',
                    ],
                'metadata' =>
                    [
                        [
                            'key' => 'KBC.name',
                            'value' => 'auto Increment Timestamp',
                        ],
                        [
                            'key' => 'KBC.sanitizedName',
                            'value' => 'auto_Increment_Timestamp',
                        ],
                        [
                            'key' => 'KBC.schema',
                            'value' => 'dbo',
                        ],
                        [
                            'key' => 'KBC.catalog',
                            'value' => 'test',
                        ],
                        [
                            'key' => 'KBC.type',
                            'value' => 'BASE TABLE',
                        ],
                    ],
                'column_metadata' =>
                    [
                        'Weir_d_I_D' =>
                            [
                                [
                                    'key' => 'KBC.datatype.type',
                                    'value' => 'int',
                                ],
                                [
                                    'key' => 'KBC.datatype.nullable',
                                    'value' => false,
                                ],
                                [
                                    'key' => 'KBC.datatype.basetype',
                                    'value' => 'INTEGER',
                                ],
                                [
                                    'key' => 'KBC.datatype.length',
                                    'value' => '10',
                                ],
                                [
                                    'key' => 'KBC.sourceName',
                                    'value' => '_Weir%d I-D',
                                ],
                                [
                                    'key' => 'KBC.sanitizedName',
                                    'value' => 'Weir_d_I_D',
                                ],
                                [
                                    'key' => 'KBC.primaryKey',
                                    'value' => true,
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
                        'Weir_d_Na_me' =>
                            [
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
                                    'value' => '55',
                                ],
                                [
                                    'key' => 'KBC.datatype.default',
                                    'value' => '(\'mario\')',
                                ],
                                [
                                    'key' => 'KBC.sourceName',
                                    'value' => 'Weir%d Na-me',
                                ],
                                [
                                    'key' => 'KBC.sanitizedName',
                                    'value' => 'Weir_d_Na_me',
                                ],
                                [
                                    'key' => 'KBC.primaryKey',
                                    'value' => false,
                                ],
                                [
                                    'key' => 'KBC.uniqueKey',
                                    'value' => true,
                                ],
                                [
                                    'key' => 'KBC.ordinalPosition',
                                    'value' => 2,
                                ],
                            ],
                        'someInteger' =>
                            [
                                [
                                    'key' => 'KBC.datatype.type',
                                    'value' => 'int',
                                ],
                                [
                                    'key' => 'KBC.datatype.nullable',
                                    'value' => true,
                                ],
                                [
                                    'key' => 'KBC.datatype.basetype',
                                    'value' => 'INTEGER',
                                ],
                                [
                                    'key' => 'KBC.datatype.length',
                                    'value' => '10',
                                ],
                                [
                                    'key' => 'KBC.sourceName',
                                    'value' => 'someInteger',
                                ],
                                [
                                    'key' => 'KBC.sanitizedName',
                                    'value' => 'someInteger',
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
                        'someDecimal' =>
                            [
                                [
                                    'key' => 'KBC.datatype.type',
                                    'value' => 'decimal',
                                ],
                                [
                                    'key' => 'KBC.datatype.nullable',
                                    'value' => true,
                                ],
                                [
                                    'key' => 'KBC.datatype.basetype',
                                    'value' => 'NUMERIC',
                                ],
                                [
                                    'key' => 'KBC.datatype.length',
                                    'value' => '10,2',
                                ],
                                [
                                    'key' => 'KBC.sourceName',
                                    'value' => 'someDecimal',
                                ],
                                [
                                    'key' => 'KBC.sanitizedName',
                                    'value' => 'someDecimal',
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
                        'type' =>
                            [
                                [
                                    'key' => 'KBC.datatype.type',
                                    'value' => 'varchar',
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
                                    'value' => '55',
                                ],
                                [
                                    'key' => 'KBC.sourceName',
                                    'value' => 'type',
                                ],
                                [
                                    'key' => 'KBC.sanitizedName',
                                    'value' => 'type',
                                ],
                                [
                                    'key' => 'KBC.primaryKey',
                                    'value' => false,
                                ],
                                [
                                    'key' => 'KBC.uniqueKey',
                                    'value' => true,
                                ],
                                [
                                    'key' => 'KBC.ordinalPosition',
                                    'value' => 5,
                                ],
                            ],
                        'smalldatetime' =>
                            [
                                [
                                    'key' => 'KBC.datatype.type',
                                    'value' => 'smalldatetime',
                                ],
                                [
                                    'key' => 'KBC.datatype.nullable',
                                    'value' => true,
                                ],
                                [
                                    'key' => 'KBC.datatype.basetype',
                                    'value' => 'TIMESTAMP',
                                ],
                                [
                                    'key' => 'KBC.datatype.default',
                                    'value' => '(NULL)',
                                ],
                                [
                                    'key' => 'KBC.sourceName',
                                    'value' => 'smalldatetime',
                                ],
                                [
                                    'key' => 'KBC.sanitizedName',
                                    'value' => 'smalldatetime',
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
                            ],
                        'datetime' =>
                            [
                                [
                                    'key' => 'KBC.datatype.type',
                                    'value' => 'datetime',
                                ],
                                [
                                    'key' => 'KBC.datatype.nullable',
                                    'value' => false,
                                ],
                                [
                                    'key' => 'KBC.datatype.basetype',
                                    'value' => 'TIMESTAMP',
                                ],
                                [
                                    'key' => 'KBC.datatype.default',
                                    'value' => '(getdate())',
                                ],
                                [
                                    'key' => 'KBC.sourceName',
                                    'value' => 'datetime',
                                ],
                                [
                                    'key' => 'KBC.sanitizedName',
                                    'value' => 'datetime',
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
                    ],
                'columns' =>
                    [
                        'Weir_d_I_D',
                        'Weir_d_Na_me',
                        'someInteger',
                        'someDecimal',
                        'type',
                        'smalldatetime',
                        'datetime',
                    ],
            ],
            $manifest
        );

        $specialManifest = $this->dataDir . '/out/tables/' . $result['imported'][3]['outputTable'] . '.csv.manifest';
        $manifest = json_decode((string) file_get_contents($specialManifest), true);
        $this->assertEquals(
            [
                'destination' => 'in.c-main.special',
                'incremental' => false,
                'metadata' =>
                    [
                        [
                            'key' => 'KBC.name',
                            'value' => 'special',
                        ],
                        [
                            'key' => 'KBC.sanitizedName',
                            'value' => 'special',
                        ],
                        [
                            'key' => 'KBC.schema',
                            'value' => 'dbo',
                        ],
                        [
                            'key' => 'KBC.catalog',
                            'value' => 'test',
                        ],
                        [
                            'key' => 'KBC.type',
                            'value' => 'BASE TABLE',
                        ],
                    ],
                'column_metadata' =>
                    [
                        'col1' =>
                            [
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
                                    'value' => 'col1',
                                ],
                                [
                                    'key' => 'KBC.sanitizedName',
                                    'value' => 'col1',
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
                                    'value' => '1',
                                ],
                            ],
                        'col2' =>
                            [
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
                                    'value' => 'col2',
                                ],
                                [
                                    'key' => 'KBC.sanitizedName',
                                    'value' => 'col2',
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
                                    'value' => '2',
                                ],
                            ],
                    ],
                'columns' =>
                    [
                        'col1',
                        'col2',
                    ],
            ],
            $manifest
        );
    }
}
