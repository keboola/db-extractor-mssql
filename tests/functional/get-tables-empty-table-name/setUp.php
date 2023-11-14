<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    $columns = [
        'ID' => 'INT NULL',
    ];

    // create table with empty name
    $manager->createTable(' ', $columns);
};
