<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use DateTimeImmutable;
use DateTimeZone;
use Keboola\DbExtractor\Configuration\MssqlDatabaseConfig;
use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\MssqlDataType;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use Keboola\DbExtractor\Extractor\MSSQLQueryFactory;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use Keboola\DbExtractor\Metadata\MssqlMetadataProvider;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

/**
 * Covers the two-mode incremental fetching WHERE that MSSQLQueryFactory::create() reimplements
 * (Pattern B mirror of db-extractor-adapter's DefaultQueryFactory): watermark + lookback and window,
 * across every internal MSSQL incremental type, plus the binary/rowversion "not supported" guard.
 */
class QueryFactoryIncrementalFetchingTest extends TestCase
{
    private string $originalTimezone;

    protected function setUp(): void
    {
        parent::setUp();
        // Component images run in UTC; pin it so relative-window resolution is deterministic here too.
        $this->originalTimezone = date_default_timezone_get();
        date_default_timezone_set('UTC');
    }

    protected function tearDown(): void
    {
        date_default_timezone_set($this->originalTimezone);
        parent::tearDown();
    }

    /**
     * @dataProvider watermarkLookbackProvider
     */
    public function testWatermarkLookback(
        string $column,
        string $sqlType,
        array $state,
        string $lookback,
        string $resolverType,
        string $expectedWhere,
    ): void {
        $params = $this->baseParams($column, [
            'incrementalFetchingLookback' => $lookback,
        ]);

        $query = $this->buildQuery($params, $sqlType, $state, $resolverType);

        self::assertSame(
            sprintf('SELECT [%s] FROM [dbo].[test] %s', $column, $expectedWhere),
            $query,
        );
    }

    public function watermarkLookbackProvider(): array
    {
        return [
            'datetime -15s' => [
                'datetime',
                'datetime',
                ['lastFetchedRow' => '2021-01-05 13:43:27.123'],
                '15 seconds',
                'TIMESTAMP',
                "WHERE [datetime] >= CONVERT(DATETIME2, '2021-01-05 13:43:12', 120)",
            ],
            'datetime2 -15s' => [
                'datetime',
                'datetime2',
                ['lastFetchedRow' => '2021-01-05 13:43:27.123'],
                '15 seconds',
                'TIMESTAMP',
                "WHERE [datetime] >= CONVERT(DATETIME2, '2021-01-05 13:43:12', 120)",
            ],
            'smalldatetime -15s (quotable)' => [
                'smalldatetime',
                'smalldatetime',
                ['lastFetchedRow' => '2021-01-05 13:43:27'],
                '15 seconds',
                'TIMESTAMP',
                "WHERE [smalldatetime] >= '2021-01-05 13:43:12'",
            ],
            'numeric -10' => [
                'someId',
                'int',
                ['lastFetchedRow' => '100'],
                '10',
                'INTEGER',
                'WHERE [someId] >= 90',
            ],
        ];
    }

    /**
     * @dataProvider windowProvider
     */
    public function testWindow(
        string $column,
        string $sqlType,
        ?string $start,
        ?string $end,
        string $resolverType,
        string $expectedWhere,
    ): void {
        $extra = ['incrementalFetchingMode' => 'window'];
        if ($start !== null) {
            $extra['incrementalFetchingStart'] = $start;
        }
        if ($end !== null) {
            $extra['incrementalFetchingEnd'] = $end;
        }
        $params = $this->baseParams($column, $extra);

        // Window mode intentionally ignores the stored watermark; keep one in state to prove it.
        $query = $this->buildQuery($params, $sqlType, ['lastFetchedRow' => '2099-01-01 00:00:00'], $resolverType);

        self::assertSame(
            sprintf('SELECT [%s] FROM [dbo].[test] %s', $column, $expectedWhere),
            $query,
        );
    }

    public function windowProvider(): array
    {
        return [
            'datetime start only' => [
                'datetime',
                'datetime',
                '2021-01-05 13:43:13',
                null,
                'TIMESTAMP',
                "WHERE [datetime] >= CONVERT(DATETIME2, '2021-01-05 13:43:13', 120)",
            ],
            'datetime start and end' => [
                'datetime',
                'datetime',
                '2021-01-05 13:43:13',
                '2021-01-05 13:44:00',
                'TIMESTAMP',
                "WHERE [datetime] >= CONVERT(DATETIME2, '2021-01-05 13:43:13', 120) " .
                "AND [datetime] <= CONVERT(DATETIME2, '2021-01-05 13:44:00', 120)",
            ],
            'datetime2 start only' => [
                'datetime',
                'datetime2',
                '2021-01-05 13:43:13',
                null,
                'TIMESTAMP',
                "WHERE [datetime] >= CONVERT(DATETIME2, '2021-01-05 13:43:13', 120)",
            ],
            'smalldatetime start only (quotable)' => [
                'smalldatetime',
                'smalldatetime',
                '2021-01-05 13:43:13',
                null,
                'TIMESTAMP',
                "WHERE [smalldatetime] >= '2021-01-05 13:43:13'",
            ],
            'numeric start and end' => [
                'someId',
                'int',
                '50',
                '150',
                'INTEGER',
                'WHERE [someId] >= 50 AND [someId] <= 150',
            ],
        ];
    }

    public function testWindowRelativeBoundUsesInjectedNow(): void
    {
        $params = $this->baseParams('datetime', [
            'incrementalFetchingMode' => 'window',
            'incrementalFetchingStart' => '-1 hour',
        ]);

        $now = new DateTimeImmutable('2021-01-05 15:00:00', new DateTimeZone('UTC'));
        $query = $this->buildQuery($params, 'datetime', [], 'TIMESTAMP', $now);

        self::assertSame(
            'SELECT [datetime] FROM [dbo].[test] ' .
            "WHERE [datetime] >= CONVERT(DATETIME2, '2021-01-05 14:00:00', 120)",
            $query,
        );
    }

    public function testBinaryColumnWithWindowThrows(): void
    {
        $params = $this->baseParams('rowVersion', [
            'incrementalFetchingMode' => 'window',
            'incrementalFetchingStart' => '2021-01-05 13:43:13',
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('not supported');

        // No column type is threaded: common's export() would have rejected this before the query is built.
        $this->buildQuery($params, 'timestamp', [], null);
    }

    public function testBinaryColumnWithLookbackThrows(): void
    {
        $params = $this->baseParams('rowVersion', [
            'incrementalFetchingLookback' => '10',
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('not supported');

        // No column type is threaded: common's export() would have rejected this before the query is built.
        $this->buildQuery($params, 'timestamp', ['lastFetchedRow' => '0x0000000000000FA0'], null);
    }

    public function testBinaryColumnPlainWatermarkStillWorks(): void
    {
        $params = $this->baseParams('rowVersion', []);

        $query = $this->buildQuery(
            $params,
            'timestamp',
            ['lastFetchedRow' => '0x0000000000000FA0'],
            null,
        );

        // A rowversion/"timestamp" column is rendered via CONVERT in the SELECT, but the WHERE inlines the
        // binary watermark token raw (no CONVERT/quote) — plain watermark fetching still works for binary.
        self::assertSame(
            'SELECT CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), [rowVersion]), 1) AS [rowVersion] ' .
            'FROM [dbo].[test] WHERE [rowVersion] >= 0x0000000000000FA0',
            $query,
        );
    }

    /**
     * @param array<string, mixed> $extra
     * @return array<string, mixed>
     */
    private function baseParams(string $incrementalColumn, array $extra): array
    {
        return array_merge([
            'table' => ['tableName' => 'test', 'schema' => 'dbo'],
            'incremental' => true,
            'incrementalFetchingColumn' => $incrementalColumn,
            'query' => null,
            'columns' => [],
            'outputTable' => 'output',
            'primaryKey' => [],
            'retries' => 3,
        ], $extra);
    }

    /**
     * @param array<string, mixed> $params
     * @param array<string, mixed> $state
     */
    private function buildQuery(
        array $params,
        string $sqlType,
        array $state,
        ?string $resolverColumnType,
        ?DateTimeImmutable $now = null,
    ): string {
        $logger = new Logger('mssql-extractor-test');
        $pdo = new MSSQLPdoConnection($logger, MssqlDatabaseConfig::fromArray(PdoTestConnection::getDbConfigArray()));

        $column = $params['incrementalFetchingColumn'];
        assert(is_string($column));
        $metadataProvider = $this->mockMetadataProvider($column, $sqlType);

        $factory = new MSSQLQueryFactory($state, $metadataProvider, null, $now);
        $factory->setFormat(MSSQLQueryFactory::ESCAPING_TYPE_PDO);
        $factory->setIncrementalFetchingType(MssqlDataType::getIncrementalFetchingType($column, $sqlType));

        $exportConfig = MssqlExportConfig::fromArray($params);
        if ($resolverColumnType !== null) {
            $exportConfig = $exportConfig->withIncrementalColumnType($resolverColumnType);
        }

        return $factory->create($exportConfig, $pdo);
    }

    private function mockMetadataProvider(string $column, string $sqlType): MssqlMetadataProvider
    {
        $table = TableBuilder::create()
            ->setName('test')
            ->setType('BASE TABLE');
        $table->addColumn()->setName($column)->setType($sqlType);

        $mock = $this->createMock(MssqlMetadataProvider::class);
        $mock->method('getTable')->willReturn($table->build());
        /** @var MssqlMetadataProvider $mock */
        return $mock;
    }
}
