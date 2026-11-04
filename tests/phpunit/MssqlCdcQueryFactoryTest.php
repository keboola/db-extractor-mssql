<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\MssqlCdcQueryFactory;
use PHPUnit\Framework\TestCase;

/**
 * Regression tests for CFTL-684.
 *
 * CDC mode failed with the misleading SQL Server error "An insufficient number of arguments were
 * supplied for the procedure or function cdc.fn_cdc_get_net_changes_ ..." (Msg 313). Microsoft
 * documents that a CDC table-valued function reports Msg 313 when an LSN endpoint is NULL or
 * outside the capture instance validity interval, because the TVF cannot raise its own error.
 *
 * These tests assert that the generated batch can never reach the TVF with such an endpoint.
 */
class MssqlCdcQueryFactoryTest extends TestCase
{
    private const CDC_NAME = 'dbo_cdc_test_table';

    private const SINCE_TIME = '2023-08-10 13:43:27.123';

    private MssqlCdcQueryFactory $factory;

    protected function setUp(): void
    {
        $this->factory = new MssqlCdcQueryFactory();
    }

    private function getQuery(): string
    {
        return $this->factory->getNetChangesQuery(self::CDC_NAME, '[a], [b]', self::SINCE_TIME);
    }

    public function testToLsnHasNullFallback(): void
    {
        // Before the fix @to_lsn was assigned the bare fn_cdc_map_time_to_lsn() result, which is
        // NULL whenever cdc.lsn_time_mapping holds no matching row (capture job not running).
        $this->assertStringContainsString(
            "SET @to_lsn = ISNULL(sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time), @max_lsn);",
            $this->getQuery(),
        );
    }

    public function testFromLsnHasNullFallback(): void
    {
        $this->assertStringContainsString(
            'SET @from_lsn = ISNULL('
                . "sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', @begin_time), @min_lsn);",
            $this->getQuery(),
        );
    }

    /**
     * The core defect: "IF @to_lsn < @from_lsn" is UNKNOWN - not TRUE - when a bound is NULL,
     * so the guard did not fire and the NULL reached the TVF. An explicit IS NULL test is needed.
     *
     * @dataProvider guardedConditionsProvider
     */
    public function testBatchGuardsEveryEndpointFailureMode(string $condition): void
    {
        $this->assertStringContainsString($condition, $this->getQuery());
    }

    /**
     * @return array<string, string[]>
     */
    public function guardedConditionsProvider(): array
    {
        return [
            // NULL bounds - a comparison alone never catches these.
            'from_lsn is null' => ['@from_lsn IS NULL'],
            'to_lsn is null' => ['@to_lsn IS NULL'],
            // fn_cdc_get_max_lsn() is itself NULL when the capture job has never run.
            'max_lsn is null' => ['@max_lsn IS NULL'],
            'min_lsn is null' => ['@min_lsn IS NULL'],
            // fn_cdc_get_min_lsn() returns all zeros - NOT NULL - for an unknown or unreadable
            // capture instance, which is outside the validity interval and also yields Msg 313.
            'min_lsn is the empty lsn' => ['@min_lsn = ' . MssqlCdcQueryFactory::EMPTY_LSN],
            // Endpoints outside the validity interval, eg. state older than CDC retention.
            'from_lsn below validity interval' => ['@from_lsn < @min_lsn'],
            'to_lsn above validity interval' => ['@to_lsn > @max_lsn'],
            // The pre-existing inverted-window guard must survive.
            'inverted window' => ['@to_lsn < @from_lsn'],
        ];
    }

    /**
     * The TVF call must sit in the ELSE branch. If it is ever emitted unconditionally again, a
     * bad endpoint reaches SQL Server and the Msg 313 regression is back.
     */
    public function testNetChangesTvfIsOnlyReachableInTheElseBranch(): void
    {
        $this->assertMatchesRegularExpression(
            '~\nELSE\nBEGIN\nSELECT .+ FROM cdc\.fn_cdc_get_net_changes_' . self::CDC_NAME . '\('
                . "@from_lsn, @to_lsn, 'all'\);\nEND~",
            $this->getQuery(),
            'The net changes TVF must be emitted inside the final ELSE branch.',
        );
    }

    public function testHappyPathStatementIsUnchanged(): void
    {
        // Backwards compatibility: when every bound is usable the executed statement is exactly
        // what the extractor emitted before the fix, so working CDC configs are unaffected.
        $this->assertStringContainsString(
            'SELECT [a], [b], IIF(__$operation = 1, 1, 0) as KBC__DELETED '
                . "FROM cdc.fn_cdc_get_net_changes_dbo_cdc_test_table(@from_lsn, @to_lsn, 'all');",
            $this->getQuery(),
        );
    }

    public function testBeginTimeUsesTheStoredState(): void
    {
        $this->assertStringContainsString(
            "SET @begin_time = CONVERT(DATETIME, '" . self::SINCE_TIME . "');",
            $this->getQuery(),
        );
    }

    public function testRangeErrorNamesTheCaptureInstance(): void
    {
        $query = $this->getQuery();

        $this->assertStringContainsString(MssqlCdcQueryFactory::ERROR_LSN_RANGE_NOT_AVAILABLE, $query);
        $this->assertStringContainsString('for capture instance ' . self::CDC_NAME, $query);
    }

    public function testInvertedWindowKeepsItsOriginalMessage(): void
    {
        // MSSQL::export() matches on this text to fall back to a full load; it must not drift.
        $this->assertStringContainsString(
            "RAISERROR('The end LSN is less than the start LSN.', 16, 1);",
            $this->getQuery(),
        );
    }

    /**
     * @dataProvider recoverableErrorProvider
     */
    public function testIsRecoverableLsnError(string $message, bool $expected): void
    {
        $this->assertSame($expected, MssqlCdcQueryFactory::isRecoverableLsnError($message));
    }

    /**
     * @return array<string, array{string, bool}>
     */
    public function recoverableErrorProvider(): array
    {
        return [
            'inverted window' => ['DB query failed: The end LSN is less than the start LSN.', true],
            'range not available' => [
                'DB query failed: The CDC LSN range is not available for capture instance dbo_t.',
                true,
            ],
            // The raw Msg 313 must NOT be treated as recoverable: after the fix the extractor
            // raises its own message first, so a 313 still reaching PHP means something else.
            'raw msg 313' => [
                'An insufficient number of arguments were supplied for the procedure or function '
                    . 'cdc.fn_cdc_get_net_changes_ ...',
                false,
            ],
            'unrelated error' => ['SQLSTATE[08001]: Login timeout expired', false],
        ];
    }

    public function testLastFetchedTimeQuery(): void
    {
        $this->assertStringContainsString(
            'SELECT sys.fn_cdc_map_lsn_to_time(@to_lsn) as last_fetched_time;',
            $this->factory->getLastFetchedTimeQuery(),
        );
    }
}
