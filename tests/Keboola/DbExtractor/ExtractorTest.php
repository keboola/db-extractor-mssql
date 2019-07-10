<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\MSSQL;
use Keboola\DbExtractor\Logger;

class ExtractorTest extends AbstractMSSQLTest
{
    /** @var array */
    private $config;

    public function setUp(): void
    {
        $this->config = $this->getConfig('mssql');
        $this->config['parameters']['extractor_class'] = 'MSSQL';
    }

    /**
     * @dataProvider simpleTableColumnsDataProvider
     */
    public function testGetSimplifiedPdoQuery(array $params, array $state, string $expected): void
    {
        $extractor = new MSSQL($this->config['parameters'], $state, new Logger('mssql-extractor-test'));

        if (isset($params['incrementalFetchingColumn']) && $params['incrementalFetchingColumn'] !== "") {
            $extractor->validateIncrementalFetching(
                $params['table'],
                $params['incrementalFetchingColumn'],
                isset($params['incrementalFetchingLimit']) ? $params['incrementalFetchingLimit'] : null
            );
        }
        $query = $extractor->getSimplePdoQuery($params['table'], $params['columns']);
        $this->assertEquals($expected, $query);
    }

    /**
     * @dataProvider bcpTableColumnsDataProvider
     */
    public function testGetBCPQuery(array $params, array $state, string $expected): void
    {
        $extractor = new MSSQL($this->config['parameters'], $state, new Logger('mssql-extractor-test'));

        if (isset($params['incrementalFetchingColumn']) && $params['incrementalFetchingColumn'] !== "") {
            $extractor->validateIncrementalFetching(
                $params['table'],
                $params['incrementalFetchingColumn'],
                isset($params['incrementalFetchingLimit']) ? $params['incrementalFetchingLimit'] : null
            );
        }
        $query = $extractor->simpleQuery($params['table'], $params['columns'] ?? []);
        $this->assertEquals($expected, $query);
    }

    /**
     * @dataProvider columnTypeProvider
     * @param array $column
     * @param array $sxpectedSql
     */
    public function testColumnCasting(array $column, array $expectedSql): void
    {
        $extractor = new MSSQL($this->config['parameters'], [], new Logger('mssql-extractor-test'));
        $this->assertEquals($expectedSql['bcp'], $extractor->columnToBcpSql($column));
        $this->assertEquals($expectedSql['pdo'], $extractor->columnToPdoSql($column));
    }

    public function columnTypeProvider(): array
    {
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
                    'bcp' => 'char(34) + COALESCE(REPLACE(CAST([xmlCol] as nvarchar(max)), char(34), char(34) + char(34)),\'\') + char(34) AS [xmlCol]',
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
                    'bcp' => 'char(34) + COALESCE(REPLACE(CAST([textCol] as nvarchar(max)), char(34), char(34) + char(34)),\'\') + char(34) AS [textCol]',
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
                    'bcp' => 'char(34) + COALESCE(REPLACE([nvarCol], char(34), char(34) + char(34)),\'\') + char(34) AS [nvarCol]',
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
    }

    public function simpleTableColumnsDataProvider(): array
    {
        return [
            'simple table select with no column metadata' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [],
                ],
                [],
                "SELECT * FROM [testSchema].[test]",
            ],
            'simple table select with all columns (columns as null)' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => null,
                ],
                [],
                "SELECT * FROM [testSchema].[test]",
            ],
            'simple table with 2 columns selected' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => array (
                        0 =>
                            array (
                                'name' => 'col1',
                                'sanitizedName' => 'col1',
                                'type' => 'varchar',
                                'length' => '21474',
                            ),
                        1 =>
                            array (
                                'name' => 'col2',
                                'sanitizedName' => 'col2',
                                'type' => 'nvarchar',
                                'length' => '2147',
                            ),
                    ),
                ],
                [],
                "SELECT [col1], [col2] FROM [testSchema].[test]",
            ],
            'simple table with text column and xml column selected' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => array (
                        0 =>
                            array (
                                'name' => 'col1',
                                'sanitizedName' => 'col1',
                                'type' => 'text',
                                'length' => '21474',
                            ),
                        1 =>
                            array (
                                'name' => 'col2',
                                'sanitizedName' => 'col2',
                                'type' => 'xml',
                                'length' => '2147',
                            ),
                    ),
                ],
                [],
                "SELECT CAST([col1] as nvarchar(max)) AS [col1], CAST([col2] as nvarchar(max)) AS [col2] FROM [testSchema].[test]",
            ],
            'test simplePDO query with limit and datetime column but no state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => $this->getColumnMetadataForIncrementalFetchingTests(),
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [],
                "SELECT TOP 10 [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], [smalldatetime], [datetime], CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] FROM [dbo].[auto Increment Timestamp] ORDER BY [datetime]",
            ],
            'test simplePDO query with limit and idp column and previos state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => $this->getColumnMetadataForIncrementalFetchingTests(),
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    "lastFetchedRow" => 4,
                ],
                "SELECT TOP 10 [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], [smalldatetime], [datetime], CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4 ORDER BY [_Weir%d I-D]",
            ],
            'test simplePDO query datetime column but no state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => $this->getColumnMetadataForIncrementalFetchingTests(),
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [],
                "SELECT [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], [smalldatetime], [datetime], CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] FROM [dbo].[auto Increment Timestamp]",
            ],
            'test simplePDO query id column and previos state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => $this->getColumnMetadataForIncrementalFetchingTests(),
                    'incrementalFetchingLimit' => 0,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    "lastFetchedRow" => 4,
                ],
                "SELECT [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], [smalldatetime], [datetime], CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4",
            ],
            'test simplePDO query datetime column and previos state and limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => $this->getColumnMetadataForIncrementalFetchingTests(),
                    'incrementalFetchingLimit' => 1000,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [
                    "lastFetchedRow" => '2018-10-26 10:52:32',
                ],
                "SELECT TOP 1000 [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], [smalldatetime], [datetime], CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] FROM [dbo].[auto Increment Timestamp] WHERE [datetime] >= '2018-10-26 10:52:32' ORDER BY [datetime]",
            ],
            'test simplePDO query datetime column and previos state and limit and NOLOCK' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                        'nolock' => 'true',
                    ],
                    'columns' => $this->getColumnMetadataForIncrementalFetchingTests(),
                    'incrementalFetchingLimit' => 1000,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [
                    "lastFetchedRow" => '2018-10-26 10:52:32',
                ],
                "SELECT TOP 1000 [_Weir%d I-D], [Weir%d Na-me], [someInteger], [someDecimal], [type], [smalldatetime], [datetime], CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestamp]), 1) AS [timestamp] FROM [dbo].[auto Increment Timestamp] WITH(NOLOCK) WHERE [datetime] >= '2018-10-26 10:52:32' ORDER BY [datetime]",
            ],
        ];
    }

    public function bcpTableColumnsDataProvider(): array
    {
        return [
            'simple table select with all columns' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => array (
                        0 =>
                            array (
                                'name' => 'col1',
                                'sanitizedName' => 'col1',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 1,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                        1 =>
                            array (
                                'name' => 'col2',
                                'sanitizedName' => 'col2',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 2,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                    ),
                ],
                [],
                "SELECT char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col1], char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col2] FROM [testSchema].[test]",
            ],
            'simple table with 1 columns selected' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => array (
                        0 =>
                            array (
                                'name' => 'col1',
                                'sanitizedName' => 'col1',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 1,
                                'primaryKey' => false,
                                'default' => null,
                            )
                    ),
                ],
                [],
                "SELECT char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col1] FROM [testSchema].[test]",
            ],
            'test query with limit and datetime column but no state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => array (
                        0 =>
                            array (
                                'name' => 'col1',
                                'sanitizedName' => 'col1',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 1,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                        1 =>
                            array (
                                'name' => 'col2',
                                'sanitizedName' => 'col2',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 2,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                    ),
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [],
                "SELECT TOP 10 char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col1], char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col2] FROM [dbo].[auto Increment Timestamp] ORDER BY [datetime]",
            ],
            'test query with limit and idp column and previos state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => array (
                        0 =>
                            array (
                                'name' => 'col1',
                                'sanitizedName' => 'col1',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 1,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                        1 =>
                            array (
                                'name' => 'col2',
                                'sanitizedName' => 'col2',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 2,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                    ),
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    "lastFetchedRow" => 4,
                ],
                "SELECT TOP 10 char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col1], char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col2] FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4 ORDER BY [_Weir%d I-D]",
            ],
            'test query datetime column but no state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => array (
                        0 =>
                            array (
                                'name' => 'col1',
                                'sanitizedName' => 'col1',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 1,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                        1 =>
                            array (
                                'name' => 'col2',
                                'sanitizedName' => 'col2',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 2,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                    ),
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [],
                "SELECT char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col1], char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col2] FROM [dbo].[auto Increment Timestamp]",
            ],
            'test simplePDO query id column and previos state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => array (
                        0 =>
                            array (
                                'name' => 'col1',
                                'sanitizedName' => 'col1',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 1,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                        1 =>
                            array (
                                'name' => 'col2',
                                'sanitizedName' => 'col2',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 2,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                    ),
                    'incrementalFetchingLimit' => 0,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    "lastFetchedRow" => 4,
                ],
                "SELECT char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col1], char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col2] FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4",
            ],
            'test query with NOLOCK' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                        'nolock' => 'true',
                    ],
                    'columns' => array (
                        0 =>
                            array (
                                'name' => 'col1',
                                'sanitizedName' => 'col1',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 1,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                        1 =>
                            array (
                                'name' => 'col2',
                                'sanitizedName' => 'col2',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 2,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                    ),
                    'incrementalFetchingLimit' => 0,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    "lastFetchedRow" => 4,
                ],
                "SELECT char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col1], char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col2] FROM [dbo].[auto Increment Timestamp] WITH(NOLOCK) WHERE [_Weir%d I-D] >= 4",
            ],
            'test query with timestamp datatype column' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => array (
                        0 =>
                            array (
                                'name' => 'col1',
                                'sanitizedName' => 'col1',
                                'type' => 'text',
                                'length' => '2147483647',
                                'nullable' => true,
                                'ordinalPosition' => 1,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                        1 =>
                            array (
                                'name' => 'timestampCol',
                                'sanitizedName' => 'timestampCol',
                                'type' => 'timestamp',
                                'length' => null,
                                'nullable' => true,
                                'ordinalPosition' => 2,
                                'primaryKey' => false,
                                'default' => null,
                            ),
                    ),
                ],
                [],
                "SELECT char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) AS [col1], CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [timestampCol]), 1) AS [timestampCol] FROM [testSchema].[test]",
            ],
        ];
    }

    private function getColumnMetadataForIncrementalFetchingTests(): array
    {
        return array (
            0 =>
                array (
                    'name' => '_Weir%d I-D',
                    'sanitizedName' => 'Weir_d_I_D',
                    'type' => 'int',
                    'length' => '10',
                    'nullable' => false,
                    'ordinalPosition' => 1,
                    'primaryKey' => true,
                    'primaryKeyName' => 'PK_AUTOINC',
                    'autoIncrement' => true,
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
                ),
            5 =>
                array (
                    'name' => 'smalldatetime',
                    'sanitizedName' => 'smalldatetime',
                    'type' => 'smalldatetime',
                    'length' => null,
                    'nullable' => false,
                    'ordinalPosition' => 6,
                    'primaryKey' => false,
                ),
            6 =>
                array (
                    'name' => 'datetime',
                    'sanitizedName' => 'datetime',
                    'type' => 'datetime',
                    'length' => null,
                    'nullable' => false,
                    'ordinalPosition' => 7,
                    'primaryKey' => false,
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
                ),
        );
    }
}
