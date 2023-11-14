<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractor\Extractor\MssqlDataType;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use Keboola\DbExtractor\Extractor\MSSQLQueryFactory;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use Keboola\DbExtractor\Metadata\MssqlMetadataProvider;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use LogicException;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class QueryFactoryTest extends TestCase
{
    use ConfigTrait;

    /**
     * @dataProvider simpleTableColumnsDataProvider
     */
    public function testGetSimplifiedPdoQuery(
        array $params,
        ?array $columnsMetadata,
        array $state,
        string $expected,
    ): void {
        $params['db'] = PdoTestConnection::getDbConfigArray();
        $params['query'] = $params['query'] ?? null;
        $params['columns'] = [];
        $params['outputTable'] = 'output';
        $params['primaryKey'] = [];
        $params['retries'] = 3;

        $incrementalFetchingType = $this->getIncrementalFetchingType($params, $columnsMetadata);

        $logger = new Logger('mssql-extractor-test');
        $pdo = new MSSQLPdoConnection($logger, DatabaseConfig::fromArray($params['db']));

        $queryFactory = $this->createQueryFactory($pdo, $state, $columnsMetadata);
        $queryFactory->setFormat(MSSQLQueryFactory::ESCAPING_TYPE_PDO);
        if ($incrementalFetchingType) {
            $queryFactory->setIncrementalFetchingType($incrementalFetchingType);
        }

        $exportConfig = MssqlExportConfig::fromArray($params);

        $query = $queryFactory->create(
            $exportConfig,
            $pdo,
        );
        $this->assertEquals($expected, $query);
    }

    /**
     * @dataProvider bcpTableColumnsDataProvider
     */
    public function testGetBCPQuery(array $params, ?array $columnsMetadata, array $state, string $expected): void
    {
        $params['db'] = PdoTestConnection::getDbConfigArray();
        $params['query'] = $params['query'] ?? null;
        $params['columns'] = [];
        $params['outputTable'] = 'output';
        $params['primaryKey'] = [];
        $params['retries'] = 3;

        $logger = new Logger('mssql-extractor-test');
        $pdo = new MSSQLPdoConnection($logger, DatabaseConfig::fromArray($params['db']));

        $incrementalFetchingType = $this->getIncrementalFetchingType($params, $columnsMetadata);

        $queryFactory = $this->createQueryFactory($pdo, $state, $columnsMetadata);
        $queryFactory->setFormat(MSSQLQueryFactory::ESCAPING_TYPE_BCP);
        if ($incrementalFetchingType) {
            $queryFactory->setIncrementalFetchingType($incrementalFetchingType);
        }

        $exportConfig = MssqlExportConfig::fromArray($params);
        $query = $queryFactory->create(
            $exportConfig,
            $pdo,
        );
        $this->assertEquals($expected, $query);
    }

    /**
     * @dataProvider columnTypeProvider
     */
    public function testColumnCasting(array $columnData, array $expectedSql): void
    {
        $logger = new Logger('mssql-extractor-test');
        $pdo = new MSSQLPdoConnection($logger, DatabaseConfig::fromArray(PdoTestConnection::getDbConfigArray()));

        $queryFactory = $this->createQueryFactory($pdo, []);

        $column = ColumnBuilder::create()
            ->setName($columnData['name'])
            ->setType($columnData['type'])
            ->build();
        $this->assertEquals($expectedSql['bcp'], $queryFactory->columnToBcpSql($column, $pdo));
        $this->assertEquals($expectedSql['pdo'], $queryFactory->columnToPdoSql($column, $pdo));
    }

    public function columnTypeProvider(): array
    {
        // @codingStandardsIgnoreStart
        return [
            'timestamp column' => [
                [
                    'name' => 'timestampCol',
                    'type' => 'timestamp',
                    'basetype' => 'STRING',
                ],
                [
                    'pdo' => 'CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestampCol]), 1) AS [timestampCol]',
                    'bcp' => 'CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestampCol]), 1) AS [timestampCol]',
                ],
            ],
            'xml column' => [
                [
                    'name' => 'xmlCol',
                    'type' => 'xml',
                    'basetype' => 'STRING',
                ],
                [
                    'pdo' => 'CAST([xmlCol] as nvarchar(max)) AS [xmlCol]',
                    'bcp' =>
                        'char(34) + ' .
                        'COALESCE(REPLACE(CAST([xmlCol] as nvarchar(max)), char(34), char(34) + char(34)),\'\') ' .
                        '+ char(34) AS [xmlCol]',
                ],
            ],
            'text column' => [
                [
                    'name' => 'textCol',
                    'type' => 'text',
                    'basetype' => 'STRING',
                ],
                [
                    'pdo' => 'CAST([textCol] as nvarchar(max)) AS [textCol]',
                    'bcp' =>
                        'char(34) + ' .
                        'COALESCE(REPLACE(CAST([textCol] as nvarchar(max)), char(34), char(34) + char(34)),\'\') ' .
                        '+ char(34) AS [textCol]',
                ],
            ],
            'int column' => [
                [
                    'name' => 'intCol',
                    'type' => 'int',
                    'basetype' => 'INTEGER',
                ],
                [
                    'pdo' => '[intCol]',
                    'bcp' => '[intCol]',
                ],
            ],
            'nvarchar column' => [
                [
                    'name' => 'nvarCol',
                    'type' => 'nvarchar',
                    'basetype' => 'STRING',
                ],
                [
                    'pdo' => '[nvarCol]',
                    'bcp' =>
                        'char(34) + ' .
                        'COALESCE(REPLACE([nvarCol], char(34), char(34) + char(34)),\'\') ' .
                        '+ char(34) AS [nvarCol]',
                ],
            ],
            'datetime column' => [
                [
                    'name' => 'datetimeCol',
                    'type' => 'datetime',
                    'basetype' => 'TIMESTAMP',
                ],
                [
                    'pdo' => '[datetimeCol]',
                    'bcp' => 'CONVERT(DATETIME2(0),[datetimeCol]) AS [datetimeCol]',
                ],
            ],
            'smalldatetime column' => [
                [
                    'name' => 'smalldatetimeCol',
                    'type' => 'smalldatetime',
                    'basetype' => 'TIMESTAMP',
                ],
                [
                    'pdo' => '[smalldatetimeCol]',
                    'bcp' => '[smalldatetimeCol]',
                ],
            ],
            'money column' => [
                [
                    'name' => 'moneyCol',
                    'type' => 'money',
                    'basetype' => 'NUMERIC',
                ],
                [
                    'pdo' => '[moneyCol]',
                    'bcp' => '[moneyCol]',
                ],
            ],
        ];
        // @codingStandardsIgnoreEnd
    }

    public function simpleTableColumnsDataProvider(): array
    {
        // @codingStandardsIgnoreStart
        return [
            'simple table with 2 columns selected' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                ],
                [
                    [
                        'name' => 'col1',
                        'type' => 'varchar',
                        'length' => '21474',
                    ],
                    [
                        'name' => 'col2',
                        'type' => 'nvarchar',
                        'length' => '2147',
                    ],
                ],
                [],
                'SELECT [col1], [col2] FROM [testSchema].[test]',
            ],
            'simple table with text column and xml column selected' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                ],
                [
                    [
                        'name' => 'col1',
                        'type' => 'text',
                        'length' => '21474',
                    ],
                    [
                        'name' => 'col2',
                        'type' => 'xml',
                        'length' => '2147',
                    ],
                ],
                [],
                'SELECT CAST([col1] as nvarchar(max)) AS [col1], CAST([col2] as nvarchar(max)) AS [col2] ' .
                'FROM [testSchema].[test]',
            ],
            'test simplePDO query with limit and datetime column but no state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                $this->getColumnMetadataForIncrementalFetchingTests(),
                [],
                'SELECT TOP 10 [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], [smalldatetime], ' .
                '[datetime], CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] ' .
                'FROM [dbo].[auto Increment Timestamp] ORDER BY [datetime]',
            ],
            'test simplePDO query with limit and idp column and previos state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                $this->getColumnMetadataForIncrementalFetchingTests(),
                [
                    'lastFetchedRow' => 4,
                ],
                'SELECT TOP 10 [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], ' .
                '[smalldatetime], [datetime], CONVERT(NVARCHAR(MAX), ' .
                'CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] ' .
                'FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4 ORDER BY [_Weir%d I-D]',
            ],
            'test simplePDO query datetime column but no state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                $this->getColumnMetadataForIncrementalFetchingTests(),
                [],
                'SELECT [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], [smalldatetime], ' .
                '[datetime], CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] ' .
                'FROM [dbo].[auto Increment Timestamp]',
            ],
            'test simplePDO query id column and previos state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                $this->getColumnMetadataForIncrementalFetchingTests(),
                [
                    'lastFetchedRow' => 4,
                ],
                'SELECT [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], [smalldatetime], ' .
                '[datetime], CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] ' .
                'FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4',
            ],
            'test simplePDO query datetime column and previos state and limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'incrementalFetchingLimit' => 1000,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                $this->getColumnMetadataForIncrementalFetchingTests(),
                [
                    'lastFetchedRow' => '2018-10-26 10:52:32',
                ],
                'SELECT TOP 1000 [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], ' .
                '[smalldatetime], [datetime], CONVERT(NVARCHAR(MAX), ' .
                'CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] ' .
                "FROM [dbo].[auto Increment Timestamp] WHERE [datetime] >= '2018-10-26 10:52:32' ORDER BY [datetime]",
            ],
            'test simplePDO query datetime column and previos state and limit and NOLOCK' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'nolock' => true,
                    'incrementalFetchingLimit' => 1000,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                $this->getColumnMetadataForIncrementalFetchingTests(),
                [
                    'lastFetchedRow' => '2018-10-26 10:52:32',
                ],
                'SELECT TOP 1000 [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], ' .
                '[smalldatetime], [datetime], CONVERT(NVARCHAR(MAX), ' .
                'CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] ' .
                'FROM [dbo].[auto Increment Timestamp] WITH(NOLOCK) ' .
                "WHERE [datetime] >= '2018-10-26 10:52:32' ORDER BY [datetime]",
            ],
        ];
        // @codingStandardsIgnoreEnd
    }

    public function bcpTableColumnsDataProvider(): array
    {
        // @codingStandardsIgnoreStart
        return [
            'simple table select with all columns' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                ],
                [
                    [
                        'name' => 'col1',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 1,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'col2',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 2,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                ],
                [],
                'SELECT ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col1], ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col2] ' .
                'FROM [testSchema].[test]',
            ],
            'simple table with 1 columns selected' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                ],
                [
                    [
                        'name' => 'col1',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 1,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                ],
                [],
                'SELECT ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col1] ' .
                'FROM [testSchema].[test]',
            ],
            'test query with limit and datetime column but no state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [
                    [
                        'name' => 'col1',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 1,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'col2',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 2,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'datetime',
                        'type' => 'datetime',
                        'length' => null,
                        'nullable' => false,
                        'ordinalPosition' => 3,
                        'primaryKey' => false,
                    ],
                ],
                [],
                'SELECT TOP 10 ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col1], ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col2], ' .
                'CONVERT(DATETIME2(0),[datetime]) AS [datetime] ' .
                'FROM [dbo].[auto Increment Timestamp] ORDER BY [datetime]',
            ],
            'test query with limit and idp column and previos state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    [
                        'name' => '_Weir%d I-D',
                        'type' => 'int',
                        'length' => '10',
                        'nullable' => false,
                        'ordinalPosition' => 1,
                        'primaryKey' => true,
                        'primaryKeyName' => 'PK_AUTOINC',
                        'autoIncrement' => true,
                    ],
                    [
                        'name' => 'col1',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 2,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'col2',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 3,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                'SELECT TOP 10 [_Weir%d I-D], ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col1], ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col2] ' .
                'FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4 ORDER BY [_Weir%d I-D]',
            ],
            'test query datetime column but no state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [
                    [
                        'name' => 'col1',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 1,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'col2',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 2,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'datetime',
                        'type' => 'datetime',
                        'length' => null,
                        'nullable' => false,
                        'ordinalPosition' => 3,
                        'primaryKey' => false,
                    ],
                ],
                [],
                'SELECT ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col1], ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col2], ' .
                'CONVERT(DATETIME2(0),[datetime]) AS [datetime] ' .
                'FROM [dbo].[auto Increment Timestamp]',
            ],
            'test simplePDO query id column and previos state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    [
                        'name' => '_Weir%d I-D',
                        'type' => 'int',
                        'length' => '10',
                        'nullable' => false,
                        'ordinalPosition' => 1,
                        'primaryKey' => true,
                        'primaryKeyName' => 'PK_AUTOINC',
                        'autoIncrement' => true,
                    ],
                    [
                        'name' => 'col1',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 2,
                        'primaryKey' => false,
                        'default' => null,
                    ],

                    [
                        'name' => 'col2',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 3,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                'SELECT [_Weir%d I-D], ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col1], ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col2] ' .
                'FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4',
            ],
            'test query with NOLOCK' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'nolock' => true,
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    [
                        'name' => '_Weir%d I-D',
                        'type' => 'int',
                        'length' => '10',
                        'nullable' => false,
                        'ordinalPosition' => 1,
                        'primaryKey' => true,
                        'primaryKeyName' => 'PK_AUTOINC',
                        'autoIncrement' => true,
                    ],
                    [
                        'name' => 'col1',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 2,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'col2',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 3,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                'SELECT [_Weir%d I-D], ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col1], ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col2] ' .
                'FROM [dbo].[auto Increment Timestamp] WITH(NOLOCK) WHERE [_Weir%d I-D] >= 4',
            ],
            'test query with timestamp datatype column' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                ],
                [
                    [
                        'name' => 'col1',
                        'type' => 'text',
                        'length' => '2147483647',
                        'nullable' => true,
                        'ordinalPosition' => 1,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'timestampCol',
                        'type' => 'timestamp',
                        'length' => null,
                        'nullable' => true,
                        'ordinalPosition' => 2,
                        'primaryKey' => false,
                        'default' => null,
                    ],
                ],
                [],
                'SELECT ' .
                'char(34) + ' .
                "COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') " .
                '+ char(34) AS [col1], ' .
                'CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestampCol]), 1) AS [timestampCol] ' .
                'FROM [testSchema].[test]',
            ],
        ];
        // @codingStandardsIgnoreEnd
    }

    private function getColumnMetadataForIncrementalFetchingTests(): array
    {
        return [
            0 =>
                [
                    'name' => '_Weir%d I-D',
                    'type' => 'int',
                    'length' => '10',
                    'nullable' => false,
                    'ordinalPosition' => 1,
                    'primaryKey' => true,
                    'primaryKeyName' => 'PK_AUTOINC',
                    'autoIncrement' => true,
                ],
            1 =>
                [
                    'name' => 'Weir%d Na-me',
                    'type' => 'varchar',
                    'length' => '55',
                    'nullable' => false,
                    'ordinalPosition' => 2,
                    'primaryKey' => false,
                ],
            2 =>
                [
                    'name' => 'someInteger',
                    'type' => 'int',
                    'length' => '10',
                    'nullable' => true,
                    'ordinalPosition' => 3,
                    'primaryKey' => false,
                ],
            3 =>
                [
                    'name' => 'someDecimal',
                    'type' => 'decimal',
                    'length' => '10,2',
                    'nullable' => true,
                    'ordinalPosition' => 4,
                    'primaryKey' => false,
                ],
            4 =>
                [
                    'name' => 'type',
                    'type' => 'varchar',
                    'length' => '55',
                    'nullable' => true,
                    'ordinalPosition' => 5,
                    'primaryKey' => false,
                ],
            5 =>
                [
                    'name' => 'smalldatetime',
                    'type' => 'smalldatetime',
                    'length' => null,
                    'nullable' => false,
                    'ordinalPosition' => 6,
                    'primaryKey' => false,
                ],
            6 =>
                [
                    'name' => 'datetime',
                    'type' => 'datetime',
                    'length' => null,
                    'nullable' => false,
                    'ordinalPosition' => 7,
                    'primaryKey' => false,
                ],
            7 =>
                [
                    'name' => 'timestamp',
                    'type' => 'timestamp',
                    'length' => '8',
                    'nullable' => false,
                    'ordinalPosition' => 8,
                    'primaryKey' => false,
                ],
        ];
    }

    private function getIncrementalFetchingType(array $params, ?array $columnsMetadata): ?string
    {
        if (isset($params['incrementalFetchingColumn'])) {
            $incFetchingCol = $params['incrementalFetchingColumn'];
            $columns = array_filter($columnsMetadata ?? [], fn(array $data) => $data['name'] === $incFetchingCol);
            if (empty($columns)) {
                throw new LogicException(
                    sprintf('Column "%s" not found in test metadata.', $incFetchingCol),
                );
            }
            $column = array_pop($columns);
            return MssqlDataType::getIncrementalFetchingType($incFetchingCol, $column['type']);
        }

        return null;
    }

    protected function createQueryFactory(
        MSSQLPdoConnection $pdo,
        array $state,
        ?array $columnsMetadata = null,
    ): MSSQLQueryFactory {
        if ($columnsMetadata === null) {
            $metadataProvider = new MssqlMetadataProvider($pdo);
        } else {
            $tableBuilder = TableBuilder::create()
                ->setName('mocked')
                ->setType('mocked');

            foreach ($columnsMetadata as $data) {
                $tableBuilder
                    ->addColumn()
                    ->setName($data['name'])
                    ->setType($data['type'])
                    ->setLength($data['length'] ?? null);
            }

            $tableMetadata = $tableBuilder->build();
            $metadataProviderMock = $this->createMock(MssqlMetadataProvider::class);
            $metadataProviderMock
                ->method('getTable')
                ->willReturn($tableMetadata);
            /** @var MssqlMetadataProvider $metadataProvider */
            $metadataProvider = $metadataProviderMock;
        }

        return new MSSQLQueryFactory($state, $metadataProvider);
    }
}
