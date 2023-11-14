<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    // custom table
    $columns = [
        'ID' => 'INT NULL',
        'PROB_COL' => 'sql_variant DEFAULT null',
    ];
    $manager->createTable('PDO_TEST', $columns);
    $manager->insertRows(
        'PDO_TEST',
        ['ID', 'PROB_COL'],
        [
            ['', 'GETDATE()'],
            ['', null],
        ],
    );
};
