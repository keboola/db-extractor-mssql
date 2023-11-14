<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    putenv('KBC_COMPONENT_RUN_MODE=debug');
    $manager = new DatabaseManager($test->getConnection());

    // sales table
    $manager->createSalesTable();
    $manager->generateSalesRows();
    $manager->addSalesConstraint('sales', ['createdat']);
};
