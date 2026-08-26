<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());
    $manager->createAITable('datetime2-table', 'datetime2');
    $manager->generateAIRows('datetime2-table');
    $manager->addAIConstraint('datetime2-table');
};
