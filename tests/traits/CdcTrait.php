<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use PDO;
use PDOException;
use RuntimeException;

trait CdcTrait
{
    use QuoteIdentifierTrait;

    protected PDO $connection;

    public function resetCdcOnDatabase(): void
    {
        $this->connection->exec('EXECUTE sys.sp_cdc_disable_db;');
        $this->connection->exec('EXECUTE sys.sp_cdc_enable_db;');
    }

    public function enableCdcOnTable(string $tableName): void
    {
        $this->connection->exec(sprintf(
            "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = '%s', @role_name = NULL, " .
            "@filegroup_name = N'PRIMARY', @supports_net_changes = 1;",
            $tableName,
        ));
    }

    public function disableCdcOnTable(string $tableName): void
    {
        $this->connection->exec(sprintf(
            "EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = '%s', @capture_instance = 'all';",
            $tableName,
        ));
    }

    public function removeCdcCaptureJob(): void
    {
        try {
            $this->connection->exec("EXEC sys.sp_cdc_stop_job @job_type = N'capture';");
        } catch (PDOException $e) {
            // sp_cdc_enable_table starts the capture job asynchronously, so it may not be running yet
            if (!str_contains($e->getMessage(), 'not currently running')) {
                throw $e;
            }
        }
        // A stopped job that has not started yet would still start later, a dropped one cannot
        $this->connection->exec("EXEC sys.sp_cdc_drop_job @job_type = N'capture';");
    }

    /**
     * The capture job reads the transaction log asynchronously, so a change reaches the change table later.
     */
    public function waitForCdcChanges(string $captureInstance, int $expectedRows, int $timeoutSeconds = 60): void
    {
        $deadline = time() + $timeoutSeconds;
        do {
            /** @var \PDOStatement $stmt */
            $stmt = $this->connection->query(sprintf(
                'SELECT COUNT(*) FROM [cdc].%s',
                $this->quoteIdentifier($captureInstance . '_CT'),
            ));
            $count = $stmt->fetchColumn();
            if (is_numeric($count) && (int) $count >= $expectedRows) {
                return;
            }
            sleep(1);
        } while (time() < $deadline);

        throw new RuntimeException(sprintf(
            'The CDC capture job did not write %d rows for capture instance "%s" within %d seconds.',
            $expectedRows,
            $captureInstance,
            $timeoutSeconds,
        ));
    }

    public function getServerTime(): string
    {
        /** @var \PDOStatement $stmt */
        $stmt = $this->connection->query('SELECT CONVERT(VARCHAR(23), GETDATE(), 121)');
        $serverTime = $stmt->fetchColumn();
        assert(is_string($serverTime));
        return $serverTime;
    }
}
