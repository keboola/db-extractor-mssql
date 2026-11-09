<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\Adapter\Exception\InvalidStateException;
use Keboola\DbExtractor\Adapter\PDO\PdoQueryMetadata;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\Extractor\Adapters\MSSQLPdoQueryMetadata;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use Throwable;

/**
 * Covers how the column metadata of a PDO export is classified when it cannot be built.
 *
 * getColumns() is called lazily by the manifest generator, i.e. after the PDO export adapter
 * has already returned, so nothing else re-classifies the exception afterwards - whatever
 * comes out of here is what the job dies with.
 *
 * These tests need no database: the columns come from PDOStatement::getColumnMeta(), which
 * is mocked.
 */
class PdoQueryMetadataErrorClassificationTest extends TestCase
{
    private const QUERY = 'SELECT [id], COUNT(*) FROM [dbo].[items] GROUP BY [id]';

    /**
     * Unchanged behaviour: a result whose columns all have a name produces exactly the same
     * ColumnCollection as the parent implementation.
     */
    public function testNamedColumnsAreBuiltAsBefore(): void
    {
        $columnsMetadata = [
            ['name' => 'id', 'native_type' => 'int'],
            ['name' => 'total', 'native_type' => 'int'],
        ];

        $columns = (new MSSQLPdoQueryMetadata($this->createStatement($columnsMetadata), self::QUERY))->getColumns();
        $expected = (new PdoQueryMetadata($this->createStatement($columnsMetadata)))->getColumns();

        $this->assertSame(['id', 'total'], $columns->getNames());
        $this->assertSame($expected->getNames(), $columns->getNames());
        $this->assertSame('int', $columns->getByName('id')->getType());
        $this->assertSame('int', $columns->getByName('total')->getType());
    }

    /**
     * An advanced query whose computed column has no alias makes SQL Server report an empty
     * column name. ColumnBuilder rejects it with a TableResultFormat InvalidArgumentException,
     * which is neither a user nor an application exception, so the job used to end with an
     * opaque "internal error" (exit code 2).
     *
     * The user can fix this by naming the column, so the job must still fail - but as a user
     * error (exit code 1) carrying the original reason and the query.
     */
    public function testEmptyColumnNameIsReportedAsUserError(): void
    {
        $metadata = new MSSQLPdoQueryMetadata(
            $this->createStatement([
                ['name' => 'id', 'native_type' => 'int'],
                ['name' => '', 'native_type' => 'int'],
            ]),
            self::QUERY,
        );

        $exception = $this->catchException($metadata);

        $this->assertInstanceOf(UserExceptionInterface::class, $exception);
        $this->assertNotInstanceOf(ApplicationExceptionInterface::class, $exception);
        $this->assertStringContainsString("Column's name cannot be empty.", $exception->getMessage());
        $this->assertStringContainsString(self::QUERY, $exception->getMessage());
    }

    /**
     * Unchanged behaviour: a PDO driver that does not report column names at all is not a user
     * error, so it must still surface as an application exception rather than be swept into the
     * new user-facing message.
     */
    public function testMissingMetadataKeyStaysApplicationError(): void
    {
        $metadata = new MSSQLPdoQueryMetadata(
            $this->createStatement([['native_type' => 'int']]),
            self::QUERY,
        );

        $exception = $this->catchException($metadata);

        $this->assertInstanceOf(InvalidStateException::class, $exception);
        $this->assertInstanceOf(ApplicationExceptionInterface::class, $exception);
        $this->assertNotInstanceOf(UserExceptionInterface::class, $exception);
    }

    /**
     * The classification only takes effect if the connection actually hands this implementation
     * to the export result. MSSQLPdoConnection connects in its constructor, so it is built
     * without one - getQueryMetadata() does not touch the connection state.
     */
    public function testConnectionProvidesTheClassifyingMetadata(): void
    {
        $connection = (new ReflectionClass(MSSQLPdoConnection::class))->newInstanceWithoutConstructor();

        $method = (new ReflectionClass(MSSQLPdoConnection::class))->getMethod('getQueryMetadata');
        $method->setAccessible(true);
        $metadata = $method->invoke($connection, self::QUERY, $this->createStatement([]));

        $this->assertInstanceOf(QueryMetadata::class, $metadata);
        $this->assertInstanceOf(MSSQLPdoQueryMetadata::class, $metadata);
    }

    private function catchException(QueryMetadata $metadata): Throwable
    {
        try {
            $metadata->getColumns();
        } catch (Throwable $e) {
            return $e;
        }

        $this->fail('Expected getColumns() to throw.');
    }

    /**
     * @param array<int, array<string, string>> $columnsMetadata
     */
    private function createStatement(array $columnsMetadata): PDOStatement
    {
        $statement = $this->createMock(PDOStatement::class);
        $statement
            ->method('columnCount')
            ->willReturn(count($columnsMetadata));
        $statement
            ->method('getColumnMeta')
            ->willReturnCallback(function (int $column) use ($columnsMetadata): array {
                return $columnsMetadata[$column];
            });

        return $statement;
    }
}
