<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    // Table carrying MS_Description extended properties on itself and on its columns
    $manager->createDescriptionsTable();
    $manager->addDescriptionsToTable();
    $manager->generateDescriptionsRows();
};
