<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Adapter\Exception\InvalidStateException;
use Keboola\DbExtractor\Extractor\MssqlPdoQueryMetadata;
use PDOStatement;
use PHPUnit\Framework\TestCase;

/**
 * Covers how the declared type is resolved from the driver metadata, without a database. The
 * cases a real SQL Server cannot produce on demand live here: a missing or empty declared type.
 */
class MssqlPdoQueryMetadataFallbackTest extends TestCase
{
    /**
     * @dataProvider columnMetadataProvider
     */
    public function testDeclaredTypeIsResolvedFromDriverMetadata(array $columnMetadata, string $expectedType): void
    {
        $columns = $this->createMetadata($columnMetadata)->getDeclaredTypeColumns();

        self::assertCount(1, $columns);
        foreach ($columns as $column) {
            self::assertSame('col', $column->getName());
            self::assertSame($expectedType, $column->getType());
        }
    }

    public function columnMetadataProvider(): array
    {
        return [
            'declared type' => [
                ['name' => 'col', 'native_type' => 'string', 'sqlsrv:decl_type' => 'decimal'],
                'decimal',
            ],
            'identity' => [
                ['name' => 'col', 'native_type' => 'string', 'sqlsrv:decl_type' => 'int identity'],
                'int',
            ],
            'identity with empty parentheses' => [
                ['name' => 'col', 'native_type' => 'string', 'sqlsrv:decl_type' => 'numeric() identity'],
                'numeric',
            ],
            'identity in upper case' => [
                ['name' => 'col', 'native_type' => 'string', 'sqlsrv:decl_type' => 'BIGINT IDENTITY'],
                'BIGINT',
            ],
            'empty declared type falls back to the native type' => [
                ['name' => 'col', 'native_type' => 'string', 'sqlsrv:decl_type' => ''],
                'string',
            ],
            'missing declared type falls back to the native type' => [
                ['name' => 'col', 'native_type' => 'LONG'],
                'LONG',
            ],
            'no type at all' => [
                ['name' => 'col'],
                'string',
            ],
        ];
    }

    public function testUnreadableColumnMetadataThrows(): void
    {
        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('Cannot read metadata of PDO query column 0.');

        $this->createMetadata(false)->getDeclaredTypeColumns();
    }

    private function createMetadata(array|false $columnMetadata): MssqlPdoQueryMetadata
    {
        $stmt = $this->createMock(PDOStatement::class);
        $stmt->method('columnCount')->willReturn(1);
        $stmt->method('getColumnMeta')->willReturn($columnMetadata);

        return new MssqlPdoQueryMetadata($stmt);
    }
}
