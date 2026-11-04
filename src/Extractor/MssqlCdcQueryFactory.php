<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

/**
 * Builds the T-SQL batch that CDC mode uses to read net changes.
 *
 * Extracted from MSSQL::export() so the generated SQL can be asserted in unit tests
 * without a live SQL Server.
 */
class MssqlCdcQueryFactory
{
    /**
     * sys.fn_cdc_get_min_lsn() returns this - not NULL - when the capture instance does not
     * exist or the caller may not read its change data.
     */
    public const EMPTY_LSN = '0x00000000000000000000';

    /** Raised when the CDC change window cannot be resolved to a usable LSN range. */
    public const ERROR_LSN_RANGE_NOT_AVAILABLE = 'The CDC LSN range is not available';

    /** Raised when the resolved change window is inverted. */
    public const ERROR_END_LSN_BEFORE_START_LSN = 'The end LSN is less than the start LSN';

    /**
     * Errors that mean "this CDC window cannot be read". A full load is a valid recovery for
     * all of them, so MSSQL::export() retries as a full load when cdcModeFullLoadFallback is on.
     *
     * @var string[]
     */
    public const RECOVERABLE_LSN_ERRORS = [
        self::ERROR_END_LSN_BEFORE_START_LSN,
        self::ERROR_LSN_RANGE_NOT_AVAILABLE,
    ];

    /**
     * @param string $cdcName   Capture instance name, "<schema>_<table>".
     * @param string $columns   Already-escaped column list for the SELECT.
     * @param string $sinceTime Value of the "lastFetchedTime" state key.
     */
    public function getNetChangesQuery(string $cdcName, string $columns, string $sinceTime): string
    {
        // Both LSN bounds must be non-NULL AND inside the capture instance validity interval
        // before cdc.fn_cdc_get_net_changes_* is called. Microsoft documents that a CDC TVF
        // cannot raise its own error, so for a null or out-of-range endpoint it instead reports
        // the misleading "An insufficient number of arguments were supplied for the procedure or
        // function cdc.fn_cdc_get_net_changes_ ..." (Msg 313) - the error seen in CFTL-684.
        // See the "Querying for change data" notes in the Microsoft "Work with change data"
        // documentation for SQL Server, which call the Msg 313 wording misleading by design.
        //
        // Three ways the old batch produced that error:
        //
        //  1. sys.fn_cdc_map_time_to_lsn() returns NULL when cdc.lsn_time_mapping has no matching
        //     row - typically when the CDC capture job is not running. @from_lsn had an ISNULL
        //     fallback, @to_lsn had none, so @to_lsn could stay NULL.
        //  2. The "IF @to_lsn < @from_lsn" guard never fired for a NULL bound: in T-SQL a
        //     comparison with NULL is UNKNOWN, not TRUE, so the NULL was passed to the TVF.
        //  3. sys.fn_cdc_get_min_lsn() returns EMPTY_LSN (all zeros), not NULL, for an unknown
        //     capture instance, so the ISNULL fallback could yield an out-of-range @from_lsn.
        //     An @from_lsn older than the retention window is rejected the same way.
        //
        // The SELECT therefore sits in an ELSE branch and is unreachable unless both bounds are
        // usable. When the bounds ARE usable the emitted statement is unchanged, so runs that
        // work today keep working. The "end LSN is less than the start LSN" text is kept stable
        // because MSSQL::export() matches on it to decide whether to fall back to a full load.
        $emptyLsn = self::EMPTY_LSN;
        $rangeError = sprintf(
            '%s for capture instance %s. The CDC capture job may not have run yet, the capture '
                . 'instance may not exist or be readable, or the stored state may be older than '
                . 'the CDC retention window.',
            self::ERROR_LSN_RANGE_NOT_AVAILABLE,
            // RAISERROR reads "%" as a format specifier, and "%" is legal in a SQL Server
            // identifier, so it must be doubled inside the message text only.
            str_replace('%', '%%', $cdcName),
        );

        // @phpcs:disable Generic.Files.LineLength
        return <<<SQL
DECLARE @begin_time datetime, @end_time datetime, @from_lsn binary(10), @to_lsn binary(10), @min_lsn binary(10), @max_lsn binary(10);
SET @begin_time = CONVERT(DATETIME, '$sinceTime');
SET @end_time = GETDATE();
SET @min_lsn = [sys].[fn_cdc_get_min_lsn]('$cdcName');
SET @max_lsn = [sys].[fn_cdc_get_max_lsn]();
SET @from_lsn = ISNULL(sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', @begin_time), @min_lsn);
SET @to_lsn = ISNULL(sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time), @max_lsn);
IF @min_lsn IS NULL OR @max_lsn IS NULL OR @min_lsn = $emptyLsn OR @from_lsn IS NULL OR @to_lsn IS NULL OR @from_lsn < @min_lsn OR @to_lsn > @max_lsn
BEGIN
RAISERROR('$rangeError', 16, 1);
END
ELSE IF @to_lsn < @from_lsn
BEGIN
RAISERROR('The end LSN is less than the start LSN.', 16, 1);
END
ELSE
BEGIN
SELECT $columns, IIF(__\$operation = 1, 1, 0) as KBC__DELETED FROM cdc.fn_cdc_get_net_changes_$cdcName(@from_lsn, @to_lsn, 'all');
END
SQL;
        // @phpcs:enable Generic.Files.LineLength
    }

    /**
     * Query that resolves the watermark stored back into state as "lastFetchedTime".
     */
    public function getLastFetchedTimeQuery(): string
    {
        return <<<SQL
DECLARE @to_lsn binary(10);
SET @to_lsn = [sys].[fn_cdc_get_max_lsn]();
SELECT sys.fn_cdc_map_lsn_to_time(@to_lsn) as last_fetched_time;
SQL;
    }

    /**
     * True when the error means the CDC window could not be read and a full load is a valid retry.
     */
    public static function isRecoverableLsnError(string $message): bool
    {
        foreach (self::RECOVERABLE_LSN_ERRORS as $error) {
            if (strpos($message, $error) !== false) {
                return true;
            }
        }

        return false;
    }
}
