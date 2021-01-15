<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Keboola\DbExtractor\FunctionalTests\DatabaseManager;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    // custom table
    $columns = [
        'ID' => 'INT NULL',
        'SMALLDATE' => 'SMALLDATETIME NOT NULL',
    ];
    $manager->createTable('SMALLDATETIME_TEST', $columns);
    $manager->insertRows(
        'SMALLDATETIME_TEST',
        ['ID', 'SMALLDATE'],
        [
            [1, 'GETDATE()'],
            [2, 'GETDATE()'],
        ]
    );
};
