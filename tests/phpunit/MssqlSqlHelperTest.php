<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Generator;
use Keboola\DbExtractor\Metadata\MssqlSqlHelper;
use Keboola\DbExtractor\Tests\Stubs\CapturingMssqlPdoConnection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class MssqlSqlHelperTest extends TestCase
{

    public function testGetDescriptionsSqlReadsBothLevelsAtOnce(): void
    {
        $sql = MssqlSqlHelper::getDescriptionsSql([], new CapturingMssqlPdoConnection(), true);

        // Only the descriptions, and only of user tables and views
        Assert::assertStringContainsString("[ep].[class] = 1 AND [ep].[name] = 'MS_Description'", $sql);
        Assert::assertStringContainsString("([o].[type]='U' OR [o].[type]='V')", $sql);
        Assert::assertStringContainsString('[o].[is_ms_shipped] = 0', $sql);

        // Keyed on (schema, table), which is what the existing name-only joins cannot do
        Assert::assertStringContainsString('INNER JOIN [sys].[schemas] AS [s]', $sql);
        Assert::assertStringContainsString('[s].[name] AS [TABLE_SCHEMA]', $sql);

        // minor_id 0 is the table itself, a higher one resolves to that column
        Assert::assertStringContainsString('[c].[column_id] = [ep].[minor_id]', $sql);
        Assert::assertStringContainsString('([ep].[minor_id] = 0 OR [c].[name] IS NOT NULL)', $sql);

        // No whitelist means no filter on the object name
        Assert::assertStringNotContainsString('[o].[name] IN', $sql);
    }

    public function testGetDescriptionsSqlSkipsColumnsWhenNotLoadingThem(): void
    {
        $sql = MssqlSqlHelper::getDescriptionsSql([], new CapturingMssqlPdoConnection(), false);

        Assert::assertStringContainsString('[ep].[minor_id] = 0', $sql);
        Assert::assertStringNotContainsString('COLUMN_NAME', $sql);
        Assert::assertStringNotContainsString('[sys].[columns]', $sql);
    }

    public function testGetDescriptionsSqlFiltersByTheWhitelist(): void
    {
        $whitelist = [
            new InputTable('users', 'dbo'),
            new InputTable('orders', 'sales'),
        ];

        $sql = MssqlSqlHelper::getDescriptionsSql($whitelist, new CapturingMssqlPdoConnection(), true);

        Assert::assertStringContainsString("[o].[name] IN ('users','orders')", $sql);
        Assert::assertStringContainsString("[s].[name] IN ('dbo','sales')", $sql);
    }

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
