<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\CreateTableTrait;

trait SimpleTableTrait
{
    use CreateTableTrait;

    public function createSimpleTable(string $name = 'simple'): void
    {
        $this->createTable($name, $this->getSimpleColumns());
    }

    protected function getSimpleColumns(): array
    {
        return [
            'id' => 'INTEGER',
            'name' => 'NVARCHAR(255) NOT NULL',
            'date' => 'DATETIME DEFAULT NULL',
        ];
    }
}
