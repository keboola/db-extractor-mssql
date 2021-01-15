<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait SpecialTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;

    public function createSpecialTable(string $name = 'special'): void
    {
        $this->createTable($name, $this->getSpecialColumns());
    }

    public function generateSpecialRows(string $tableName = 'special'): void
    {
        $data = $this->getSpecialRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    private function getSpecialRows(): array
    {
        return [
            'columns' => ['col1', 'col2'],
            'data' => [
                ['column with backslash \ inside', 'column with backslash and enclosure \"'],
                ['column with enclosure ", and comma inside text', 'second column enclosure in text "'],
                ['column with \n \t \\\\', 'second col'],
                ['columns with
new line', 'columns with 	tab'],
                ['first','something with

double new line'],
                ['line with enclosure','second column'],
                ['unicode characters','ľščťžýáíéúäôň'],
            ],
        ];
    }

    protected function getSpecialColumns(): array
    {
        return [
            'col1' => 'text null',
            'col2' => 'text null',
        ];
    }
}
