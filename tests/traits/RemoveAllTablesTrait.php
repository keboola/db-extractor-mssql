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
        // Delete all tables, except sys tables
        $sql = 'SELECT * FROM information_schema.tables';
        $stmt = $this->connection->query($sql);
        if ($stmt) {
            $tables = (array) $stmt->fetchAll(PDO::FETCH_ASSOC);
            foreach ($tables as $table) {
                $this->connection->query(sprintf('DROP TABLE %s', $this->quoteIdentifier($table['TABLE_NAME'])));
            }
        }
    }
}
