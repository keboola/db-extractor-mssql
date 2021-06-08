<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Generator;
use Keboola\DbExtractor\Metadata\MssqlSqlHelper;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class MssqlSqlHelperTest extends TestCase
{

    /**
     * @dataProvider getDefaultValueProvider
     */
    public function testGetDefaultValue(string $dataType, string $defaultValue, string $expectedDefaultValue): void
    {
        $newDefaultValue = MssqlSqlHelper::getDefaultValue($dataType, $defaultValue);

        Assert::assertEquals($expectedDefaultValue, $newDefaultValue);
    }

    public function getDefaultValueProvider(): Generator
    {
        yield 'int with bracket' => [
            'int',
            '((12345))',
            '12345',
        ];

        yield 'int without bracket' => [
            'int',
            '12345',
            '12345',
        ];
        yield 'decimal with bracket' => [
            'decimal',
            '((123.45))',
            '123.45',
        ];

        yield 'decimal without bracket' => [
            'decimal',
            '123.45',
            '123.45',
        ];

        yield 'string with bracket' => [
            'varchar',
            '((12345))',
            '((12345))',
        ];

        yield 'string without bracket' => [
            'varchar',
            '12345',
            '12345',
        ];
    }
}
