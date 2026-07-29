<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\Adapter\Exception\UserException;
use Keboola\DbExtractor\Adapter\Exception\UserRetriedException;
use Keboola\DbExtractor\Exception\BcpAdapterException;
use Keboola\DbExtractor\Extractor\Adapters\BcpQueryMetadata;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use PDOException;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use RuntimeException;
use Throwable;

/**
 * Covers how BcpQueryMetadata classifies a failure of the sp_describe_first_result_set
 * metadata query. getColumns() is called lazily by the manifest generator, i.e. after
 * BcpExportAdapter::export() has already returned, so nothing else re-classifies the
 * exception afterwards - whatever handleException() returns is what the job dies with.
 *
 * These tests need no database: handleException() is pure and the connection is mocked.
 */
class BcpQueryMetadataErrorClassificationTest extends TestCase
{
    /**
     * The connection layer retries transient DB errors (PDOException) with exponential
     * backoff and, once the retries are exhausted, reports them as UserRetriedException.
     * Such an already user-classified failure must stay a user error, otherwise the job
     * ends with an opaque "internal error" (exit code 2) instead of an actionable
     * message (exit code 1).
     */
    public function testRetriedTransientDbErrorStaysUserException(): void
    {
        $exception = $this->handleException(new UserRetriedException(
            5,
            'SQLSTATE[HYT00]: [Microsoft][ODBC Driver 17 for SQL Server]Login timeout expired',
        ));

        $this->assertInstanceOf(UserExceptionInterface::class, $exception);
        $this->assertNotInstanceOf(ApplicationExceptionInterface::class, $exception);
        $this->assertStringContainsString('Login timeout expired', $exception->getMessage());
    }

    /**
     * The same holds for the UserException that getColumns() itself raises when the
     * metadata result is incomplete - its message is written for the user.
     */
    public function testUserExceptionIsNotDowngraded(): void
    {
        $exception = $this->handleException(new UserException('Cannot retrieve all column metadata'));

        $this->assertInstanceOf(UserExceptionInterface::class, $exception);
        $this->assertNotInstanceOf(ApplicationExceptionInterface::class, $exception);
        $this->assertStringContainsString('Cannot retrieve all column metadata', $exception->getMessage());
    }

    /**
     * Unchanged behaviour: anything that is not already user-classified is still wrapped
     * in a BcpAdapterException (an ApplicationException).
     */
    public function testUnexpectedErrorStillBecomesApplicationException(): void
    {
        $exception = $this->handleException(new RuntimeException('Something unexpected'));

        $this->assertInstanceOf(BcpAdapterException::class, $exception);
        $this->assertInstanceOf(ApplicationExceptionInterface::class, $exception);
        $this->assertNotInstanceOf(UserExceptionInterface::class, $exception);
        $this->assertStringContainsString('Something unexpected', $exception->getMessage());
    }

    /**
     * Unchanged behaviour: a raw PDOException (not retried by the connection layer) is
     * still reported as a BcpAdapterException.
     */
    public function testPdoExceptionStillBecomesApplicationException(): void
    {
        $exception = $this->handleException(new PDOException('SQLSTATE[42S02]: Base table not found'));

        $this->assertInstanceOf(BcpAdapterException::class, $exception);
        $this->assertInstanceOf(ApplicationExceptionInterface::class, $exception);
        $this->assertStringContainsString('Base table not found', $exception->getMessage());
    }

    /**
     * Unchanged behaviour: the "uses a temp table" branch keeps its own dedicated
     * message and still takes precedence over the generic handling.
     */
    public function testTempTableErrorKeepsDedicatedMessage(): void
    {
        $exception = $this->handleException(new PDOException(
            '[Microsoft][ODBC Driver 17 for SQL Server][SQL Server]The metadata could not be determined '
            . "because statement 'delete from #ErrFile' uses a temp table.",
        ));

        $this->assertInstanceOf(UserException::class, $exception);
        $this->assertStringContainsString('Cannot retrieve column metadata via query', $exception->getMessage());
        $this->assertStringContainsString('uses a temp table.', $exception->getMessage());
    }

    private function handleException(Throwable $exception): Throwable
    {
        $query = 'SELECT 1';
        $object = new BcpQueryMetadata($this->createMock(MSSQLPdoConnection::class), $query);

        $method = (new ReflectionClass($object))->getMethod('handleException');
        $method->setAccessible(true);
        $result = $method->invoke($object, $exception, $query);

        $this->assertInstanceOf(Throwable::class, $result);

        return $result;
    }
}
