<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    // simple empty table
    $manager->createSimpleTable();

    // Auto increment table
    $manager->createAITable();
    $manager->generateAIRows();
    $manager->addAIConstraint();

    // sales table
    $manager->createSalesTable();
    $manager->generateSalesRows();
    $manager->addSalesConstraint('sales', ['createdat']);

    // second sales table with foreign key
    $manager->createSalesTable('sales2');
    $manager->generateSalesRows('sales2');
    $manager->addSalesConstraint('sales2');

    // special table
    $manager->createSpecialTable();
    $manager->generateSpecialRows();
};
