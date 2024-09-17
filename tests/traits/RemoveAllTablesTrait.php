<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use PDO;
use PDOException;

trait RemoveAllTablesTrait
{
    use QuoteIdentifierTrait;

    protected PDO $connection;

    protected function removeAllTables(): void
    {
        $this->removeAllFkConstraints();

        // Delete all tables, excluding system tables (sys, cdc)
        $sql = "
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM information_schema.tables
        WHERE TABLE_SCHEMA NOT IN ('sys', 'cdc')
        AND TABLE_NAME NOT LIKE 'sys%'
        AND TABLE_TYPE = 'BASE TABLE'
    ";

        /** @var \PDOStatement $stmt */
        $stmt = $this->connection->query($sql);
        /** @var array $tables */
        $tables = $stmt->fetchAll(PDO::FETCH_ASSOC);

        foreach ($tables as $table) {
            $schema = $this->quoteIdentifier($table['TABLE_SCHEMA']);
            $tableName = $this->quoteIdentifier($table['TABLE_NAME']);

            // Check if CDC is enabled by querying the system CDC tables directly
            $cdcEnabled = false;
            $captureInstance = null;
            try {
                $cdcCheckSql = sprintf(
                    "SELECT capture_instance FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('%s.%s')",
                    $table['TABLE_SCHEMA'],
                    $table['TABLE_NAME'],
                );
                $cdcCheckStmt = $this->connection->query($cdcCheckSql);
                $cdcRow = $cdcCheckStmt->fetch(PDO::FETCH_ASSOC);
                if (is_array($cdcRow) && isset($cdcRow['capture_instance'])) {
                    $cdcEnabled = true;
                    $captureInstance = $cdcRow['capture_instance'];
                }
            } catch (PDOException $e) {
                // Ignore error if CDC is not enabled
                if (!str_contains($e->getMessage(), 'Invalid object name')) {
                    throw $e;
                }
            }

            // If CDC is enabled, disable it before dropping the table
            if ($cdcEnabled && $captureInstance) {
                $disableCdcSql = sprintf(
                    "EXEC sys.sp_cdc_disable_table @source_schema = '%s', @source_name = '%s', " .
                    "@capture_instance = '%s'",
                    $table['TABLE_SCHEMA'],
                    $table['TABLE_NAME'],
                    $captureInstance,
                );
                $this->connection->exec($disableCdcSql);
            }

            // Drop the table
            $this->connection->query(sprintf('DROP TABLE %s.%s', $schema, $tableName));
        }
    }

    protected function removeAllFkConstraints(): void
    {
        $sql = 'SELECT * FROM information_schema.table_constraints WHERE CONSTRAINT_TYPE = \'FOREIGN KEY\'';
        /** @var \PDOStatement $stmt */
        $stmt = $this->connection->query($sql);
        /** @var array $tables */
        $tables = $stmt->fetchAll(PDO::FETCH_ASSOC);
        foreach ($tables as $table) {
            $this->connection->query(sprintf(
                'ALTER TABLE %s.%s DROP CONSTRAINT %s',
                $this->quoteIdentifier($table['TABLE_SCHEMA']),
                $this->quoteIdentifier($table['TABLE_NAME']),
                $this->quoteIdentifier($table['CONSTRAINT_NAME']),
            ));
        }
    }
}
