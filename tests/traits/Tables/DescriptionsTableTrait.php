<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

/**
 * A table carrying `MS_Description` extended properties on itself and on its columns.
 *
 * Deliberately a dedicated table rather than descriptions added to one of the shared
 * fixtures -- describing those would change the expected manifests of every other
 * functional test.
 */
trait DescriptionsTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;

    public function createDescriptionsTable(string $name = 'descriptions'): void
    {
        $this->createTable($name, $this->getDescriptionsColumns());
    }

    public function addDescriptionsToTable(string $name = 'descriptions', string $schema = 'dbo'): void
    {
        $this->addTableDescription($name, 'Table level description', $schema);
        $this->addColumnDescription($name, 'id', 'Surrogate key', $schema);
        $this->addColumnDescription($name, 'name', 'Customer name', $schema);

        // "note" is intentionally left undescribed, so that a description leaking onto a
        // column that has none would fail the test.

        // SQL Server, unlike Postgres, does store an empty extended property rather than
        // discarding it -- it must not surface as an empty description.
        $this->addColumnDescription($name, 'empty_description', '', $schema);
    }

    public function addTableDescription(string $table, string $description, string $schema = 'dbo'): void
    {
        $this->connection->prepare(sprintf(
            'EXEC sp_addextendedproperty '
            . "@name = N'MS_Description', @value = %s, "
            . "@level0type = N'SCHEMA', @level0name = %s, "
            . "@level1type = N'TABLE', @level1name = %s",
            $this->quoteUnicode($description),
            $this->quoteUnicode($schema),
            $this->quoteUnicode($table),
        ))->execute();
    }

    public function addColumnDescription(
        string $table,
        string $column,
        string $description,
        string $schema = 'dbo',
    ): void {
        $this->connection->prepare(sprintf(
            'EXEC sp_addextendedproperty '
            . "@name = N'MS_Description', @value = %s, "
            . "@level0type = N'SCHEMA', @level0name = %s, "
            . "@level1type = N'TABLE', @level1name = %s, "
            . "@level2type = N'COLUMN', @level2name = %s",
            $this->quoteUnicode($description),
            $this->quoteUnicode($schema),
            $this->quoteUnicode($table),
            $this->quoteUnicode($column),
        ))->execute();
    }

    public function generateDescriptionsRows(string $tableName = 'descriptions'): void
    {
        $data = $this->getDescriptionsRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    /**
     * The extended property value and the sysname arguments are nvarchar,
     * so the literals are N-prefixed
     */
    private function quoteUnicode(string $value): string
    {
        return 'N' . $this->quote($value);
    }

    private function getDescriptionsRows(): array
    {
        return [
            'columns' => ['id', 'name', 'note', 'empty_description'],
            'data' => [
                [1, 'Alice', 'first', 'x'],
                [2, 'Bob', 'second', 'y'],
            ],
        ];
    }

    private function getDescriptionsColumns(): array
    {
        return [
            'id' => 'INT NOT NULL',
            'name' => 'VARCHAR(100)',
            'note' => 'VARCHAR(100)',
            'empty_description' => 'VARCHAR(100)',
        ];
    }
}
