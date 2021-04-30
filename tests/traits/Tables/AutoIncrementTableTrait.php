<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait AutoIncrementTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use AddConstraintTrait;

    public function createAITable(string $name = 'auto Increment Timestamp', string $datetimeType = 'DATETIME'): void
    {
        $this->createTable($name, $this->getAIColumns($datetimeType));
    }

    public function generateAIRows(string $tableName = 'auto Increment Timestamp'): void
    {
        $data = $this->getAIRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    public function addAIConstraint(string $tableName = 'auto Increment Timestamp'): void
    {
        $this->addConstraint($tableName, 'PK_AUTOINC', 'PRIMARY KEY', '"_Weir%d I-D"');
        $this->addConstraint($tableName, 'UNI_KEY_1', 'UNIQUE', '"Weir%d Na-me", Type');
        $this->addConstraint(
            $tableName,
            'CHK_ID_CONTSTRAINT',
            'CHECK',
            '"_Weir%d I-D" > 0 AND "_Weir%d I-D" < 20'
        );
    }

    private function getAIRows(): array
    {
        return [
            'columns' => ['Weir%d Na-me', 'type', 'someInteger', 'someDecimal', 'smalldatetime', 'datetime'],
            'data' => [
                ['mario', 'plumber', 1, 1.1, '2012-01-10 10:00', '2021-01-05 13:43:11.000'],
                ['luigi', 'plumber', 2, 2.2, '2012-01-10 10:05', '2021-01-05 13:43:12.123'],
                ['toad', 'mushroom', 3, 3.3, '2012-01-10 10:10', '2021-01-05 13:43:13.456'],
                ['princess', 'royalty', 4, 4.4, '2012-01-10 10:15', '2021-01-05 13:43:14.489'],
                ['wario', 'badguy', 5, 5.5, '2012-01-10 10:25', '2021-01-05 13:43:15.234'],
                ['yoshi', 'horse?', 6, 6.6, '2012-01-10 10:25', '2021-01-05 13:43:27.123'],
            ],
        ];
    }

    private function getAIColumns(string $datetimeType = 'DATETIME'): array
    {
        return [
            '_Weir%d I-D' => 'INT IDENTITY(1,1) NOT NULL',
            'Weir%d Na-me' => 'VARCHAR(55) NOT NULL DEFAULT \'mario\'',
            'someInteger' => 'INT',
            'someDecimal' => 'DECIMAL(10,2)',
            'type' => 'VARCHAR(55) NULL',
            'smalldatetime' => 'SMALLDATETIME DEFAULT NULL',
            'datetime' => "$datetimeType NOT NULL DEFAULT GETDATE()",
        ];
    }
}
