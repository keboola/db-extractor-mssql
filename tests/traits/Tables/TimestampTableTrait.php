<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait TimestampTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use AddConstraintTrait;

    public function createTimestampTable(string $name = 'timestamp_test'): void
    {
        $this->createTable($name, $this->getTimestampColumns());
    }

    public function generateTimestampRows(string $tableName = 'timestamp_test'): void
    {
        $data = $this->getTimestampRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    private function getTimestampRows(): array
    {
        return [
            'columns' => [
                'id',
            ],
            // timestamp is generated value, so it is not present in insert statements
            'data' => [
                [1],
                [2],
                [3],
            ],
        ];
    }

    private function getTimestampColumns(): array
    {
        return [
            'id' => 'INT NOT NULL',
            // timestamp is very special type, it is row version
            'timestamp' => 'TIMESTAMP NOT NULL',
        ];
    }
}
