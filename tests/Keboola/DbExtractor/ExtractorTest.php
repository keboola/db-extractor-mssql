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

    public function simpleTableColumnsDataProvider(): array
    {
        return [
            'simple table select with all columns' => [
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
                    'columns' => ["col1", "col2"],
                ],
                [],
                "SELECT [col1], [col2] FROM [testSchema].[test]",
            ],
            'test simplePDO query with limit and timestamp column but no state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => [],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [],
                "SELECT TOP 10 * FROM [dbo].[auto Increment Timestamp] ORDER BY [timestamp]",
            ],
            'test simplePDO query with limit and idp column and previos state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => [],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    "lastFetchedRow" => 4,
                ],
                "SELECT TOP 10 * FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4 ORDER BY [_Weir%d I-D]",
            ],
            'test simplePDO query timestamp column but no state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => [],
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [],
                "SELECT * FROM [dbo].[auto Increment Timestamp] ORDER BY [timestamp]",
            ],
            'test simplePDO query id column and previos state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => [],
                    'incrementalFetchingLimit' => 0,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    "lastFetchedRow" => 4,
                ],
                "SELECT * FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4 ORDER BY [_Weir%d I-D]",
            ],
            'test simplePDO query timestamp column and previos state and limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                    ],
                    'columns' => [],
                    'incrementalFetchingLimit' => 1000,
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [
                    "lastFetchedRow" => '2018-10-26 10:52:32',
                ],
                "SELECT TOP 1000 * FROM [dbo].[auto Increment Timestamp] " .
                "WHERE [timestamp] >= '2018-10-26 10:52:32' ORDER BY [timestamp]",
            ],
            'test simplePDO query timestamp column and previos state and limit and NOLOCK' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'dbo',
                        'nolock' => 'true',
                    ],
                    'columns' => [],
                    'incrementalFetchingLimit' => 1000,
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [
                    "lastFetchedRow" => '2018-10-26 10:52:32',
                ],
                "SELECT TOP 1000 * FROM [dbo].[auto Increment Timestamp] WITH(NOLOCK) " .
                "WHERE [timestamp] > '2018-10-26 10:52:32' ORDER BY [timestamp]",
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
                "SELECT char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34), char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) FROM [testSchema].[test]",
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
                "SELECT char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) FROM [testSchema].[test]",
            ],
            'test simplePDO query with limit and timestamp column but no state' => [
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
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [],
                "SELECT TOP 10 char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34), char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) FROM [dbo].[auto Increment Timestamp] ORDER BY [timestamp]",
            ],
            'test simplePDO query with limit and idp column and previos state' => [
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
                "SELECT TOP 10 char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34), char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4 ORDER BY [_Weir%d I-D]",
            ],
            'test simplePDO query timestamp column but no state and no limit' => [
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
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [],
                "SELECT char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34), char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) FROM [dbo].[auto Increment Timestamp] ORDER BY [timestamp]",
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
                "SELECT char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34), char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) FROM [dbo].[auto Increment Timestamp] WHERE [_Weir%d I-D] >= 4 ORDER BY [_Weir%d I-D]",
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
                "SELECT char(34) + COALESCE(REPLACE(CAST([col1] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34), char(34) + COALESCE(REPLACE(CAST([col2] as nvarchar(max)), char(34), char(34) + char(34)),'') + char(34) FROM [dbo].[auto Increment Timestamp] WITH(NOLOCK) WHERE [_Weir%d I-D] > 4 ORDER BY [_Weir%d I-D]",
            ],
        ];
    }
}
