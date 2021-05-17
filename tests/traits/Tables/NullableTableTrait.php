<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait NullableTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use AddConstraintTrait;

    public function createNullableTable(string $name = 'datetime_with_nulls'): void
    {
        $this->createTable($name, $this->getNullableTableColumns());
    }

    public function generateNullableTableRows(string $tableName = 'datetime_with_nulls'): void
    {
        $data = $this->getNullableTableRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    private function getNullableTableRows(): array
    {
        return [
            'columns' => ['id', 'name', 'smalldatetime', 'datetime1', 'datetime2'],
            'data' => [
                [1, 'luigi', '2012-01-10 10:05', '2021-01-05 13:43:12.123', '2021-01-05 13:43:12.123'],
                [2, 'toad', '2012-01-10 10:10', '2021-01-05 13:43:13.456', '2021-01-05 13:43:13.456'],
                [null, null, null, null, '2021-01-05 13:43:14.489'],
            ],
        ];
    }

    private function getNullableTableColumns(string $datetimeType = 'DATETIME'): array
    {
        return [
            'id' => 'INT DEFAULT NULL',
            'name' => 'VARCHAR(55) DEFAULT NULL',
            'smalldatetime' => 'SMALLDATETIME DEFAULT NULL',
            'datetime1' => "$datetimeType DEFAULT NULL",
            'datetime2' => "$datetimeType DEFAULT NULL",
        ];
    }
}
