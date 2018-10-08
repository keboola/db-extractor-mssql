<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\MSSQL;
use Keboola\DbExtractor\ExtractorFactory;
use Keboola\DbExtractor\Logger;

class ExtractorTest extends AbstractMSSQLTest
{
    /** @var MSSQL */
    private $extractor;

    public function setUp(): void
    {
        $config = $this->getConfig('mssql');
        $config['parameters']['extractor_class'] = 'MSSQL';

        $extractorFactory = new ExtractorFactory($config['parameters'], []);
        $this->extractor = $extractorFactory->create(new Logger('mssql-extractor-test'));
    }

    /**
     * @dataProvider tableColumnsDataProvider
     */
    public function testGetSimplifiedPdoQuery(array $params, string $expected): void
    {
        $query = $this->extractor->getSimplePdoQuery($params['table'], $params['columns']);
        $this->assertEquals($expected, $query);
    }

    public function tableColumnsDataProvider(): array
    {
        return [
            // first test
            [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [],
                ],
                "SELECT * FROM [testSchema].[test]",
            ],
            [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => null,
                ],
                "SELECT * FROM [testSchema].[test]",
            ],
            [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => ["col1", "col2"],
                ],
                "SELECT [col1], [col2] FROM [testSchema].[test]",
            ],
        ];
    }
}
