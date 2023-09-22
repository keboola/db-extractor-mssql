<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Keboola\DbExtractor\FunctionalTests\DatabaseManager;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    // simple empty table
    $manager->createSalesTable();
    $manager->generateSalesRows();

    $manager->createView('sales_view', 'sales');
};
