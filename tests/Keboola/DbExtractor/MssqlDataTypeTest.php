<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\MssqlDataType;
use PHPUnit\Framework\TestCase;

class MssqlDataTypeTest extends TestCase
{
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
            'integer' => [
                ['name' => 'intColumn', 'type' => 'integer'],
                MssqlDataType::INCREMENT_TYPE_NUMERIC,
            ],
            'decimal' => [
                ['name' => 'decColumn', 'type' => 'decimal'],
                MssqlDataType::INCREMENT_TYPE_NUMERIC,
            ],
            'money' => [
                ['name' => 'moneyColumn', 'type' => 'money'],
                MssqlDataType::INCREMENT_TYPE_NUMERIC,
            ],
            'smalldatetime' => [
                ['name' => 'smalldatetimeColumn', 'type' => 'smalldatetime'],
                MssqlDataType::INCREMENT_TYPE_QUOTABLE,
            ],
            'float' => [
                ['name' => 'floatCol', 'type' => 'float'],
                MssqlDataType::INCREMENT_TYPE_NUMERIC,
            ],
            'datetime' => [
                ['name' => 'datetimeColumn', 'type' => 'datetime'],
                MssqlDataType::INCREMENT_TYPE_DATETIME,
            ],
            'datetime2' => [
                ['name' => 'datetime2Column', 'type' => 'datetime2'],
                MssqlDataType::INCREMENT_TYPE_DATETIME,
            ],
            'timestamp' => [
                ['name' => 'lastUpdate', 'type' => 'timestamp'],
                MssqlDataType::INCREMENT_TYPE_BINARY,
            ],
        ];
    }

    public function testInvalidIncrementalFetchingType(): void
    {
        $this->setExpectedException(UserException::class);
        MssqlDataType::getIncrementalFetchingType('varcharColumn', 'varchar');
    }
}
