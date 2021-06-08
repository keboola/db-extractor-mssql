<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Keboola\DbExtractor\FunctionalTests\DatabaseManager;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    // simple empty table
    $manager->createTable('simple', [
        'int' => 'INT DEFAULT 123',
        'decimal' => 'DECIMAL DEFAULT 123.45',
        'float' => 'FLOAT DEFAULT 987.65',
    ]);

    $manager->insertRows(
        'simple',
        ['int', 'decimal', 'float'],
        [[1, 123, 987]]
    );
};
