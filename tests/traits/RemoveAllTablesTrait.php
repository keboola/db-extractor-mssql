<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use PDO;

trait RemoveAllTablesTrait
{
    use QuoteIdentifierTrait;

    protected PDO $connection;

    protected function removeAllTables(): void
    {
        $this->removeAllFkConstraints();

        // Delete all tables, except sys tables
        $sql = 'SELECT * FROM information_schema.tables';
        /** @var \PDOStatement $stmt */
        $stmt = $this->connection->query($sql);
        /** @var array $tables */
        $tables = $stmt->fetchAll(PDO::FETCH_ASSOC);
        foreach ($tables as $table) {
            $this->connection->query(sprintf(
                'DROP TABLE %s.%s',
                $this->quoteIdentifier($table['TABLE_SCHEMA']),
                $this->quoteIdentifier($table['TABLE_NAME'])
            ));
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
                $this->quoteIdentifier($table['CONSTRAINT_NAME'])
            ));
        }
    }
}
