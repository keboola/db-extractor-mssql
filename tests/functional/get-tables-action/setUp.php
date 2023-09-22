<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Keboola\DbExtractor\FunctionalTests\DatabaseManager;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    $test->getConnection()->prepare('EXECUTE sys.sp_cdc_disable_db;')->execute();
    $test->getConnection()->prepare('EXECUTE sys.sp_cdc_enable_db;')->execute();

    $manager->createSimpleTable('cdc_test_table');
    $manager->addSimpleConstraint('cdc_test_table');

    // enable CDC for table
    $enableCdc = <<<SQL
EXEC sys.sp_cdc_enable_table
     @source_schema = 'dbo',
     @source_name = 'cdc_test_table',
     @role_name = NULL,
     @filegroup_name = N'PRIMARY',
     @supports_net_changes = 1;
SQL;
    $test->getConnection()->prepare($enableCdc)->execute();

    // simple empty table
    $manager->createSimpleTable();

    // sales table
    $manager->createSalesTable();

    // special table
    $manager->createSpecialTable();
};
