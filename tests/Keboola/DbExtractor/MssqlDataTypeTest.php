<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\MssqlDataType;

class MssqlDataTypeTest extends AbstractMSSQLTest
{
    /** @var array */
    private $config;

    public function setUp(): void
    {
        $this->config = $this->getConfig('mssql');
        $this->config['parameters']['extractor_class'] = 'MSSQL';
    }

    /**
     * @dataProvider columnDataTypeProvider
     */
    public function testIncrementalFetchingType(array $column, string $expectedType): void
    {
        $this->assertEquals(MssqlDataType::getIncrementalFetchingType($column['name'], $column['type']), $expectedType);
    }


    public function columnDataTypeProvider(): array
    {
        return [
            [
                ['name' => 'intColumn', 'type' => 'integer'],
                MssqlDataType::INCREMENT_TYPE_NUMERIC,
            ],
            [
                ['name' => 'decColumn', 'type' => 'decimal'],
                MssqlDataType::INCREMENT_TYPE_NUMERIC,
            ],
            [
                ['name' => 'moneyColumn', 'type' => 'money'],
                MssqlDataType::INCREMENT_TYPE_NUMERIC,
            ],
            [
                ['name' => 'smalldatetimeColumn', 'type' => 'smalldatetime'],
                MssqlDataType::INCREMENT_TYPE_NUMERIC,
            ],
            [
                ['name' => 'floatCol', 'type' => 'float'],
                MssqlDataType::INCREMENT_TYPE_NUMERIC,
            ],
            [
                ['name' => 'datetimeColumn', 'type' => 'datetime'],
                MssqlDataType::INCREMENT_TYPE_TIMESTAMP,
            ],
            [
                ['name' => 'datetime2Column', 'type' => 'datetime2'],
                MssqlDataType::INCREMENT_TYPE_TIMESTAMP,
            ],
        ];
    }

    public function testInvalidIncrementalFetchingType(): void
    {
        $this->setExpectedException(UserException::class);
        MssqlDataType::getIncrementalFetchingType('varcharColumn', 'varchar');
    }
}
