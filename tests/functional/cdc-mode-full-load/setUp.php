<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Keboola\DbExtractor\FunctionalTests\DatabaseManager;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    $test->getConnection()->prepare('EXECUTE sys.sp_cdc_disable_db;')->execute();
    $test->getConnection()->prepare('EXECUTE sys.sp_cdc_enable_db;')->execute();

    // Auto increment table
    $manager->createAITable('cdc_test_table');
    $manager->addAIConstraint('cdc_test_table');

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
    $manager->generateAIRows('cdc_test_table');

    $newData = [
        'columns' => ['Weir%d Na-me', 'type', 'someInteger', 'someDecimal', 'smalldatetime', 'datetime'],
        'data' => [
            ['nedData', 'horse?', 6, 6.6, '2023-08-10 10:25', '2023-08-10 13:43:27.123'],
            ['asd', 'horse?', 6, 6.6, '2023-08-10 10:25', '2023-08-10 13:43:27.123'],
        ],
    ];
    $manager->insertRows('cdc_test_table', $newData['columns'], $newData['data']);

    sleep(6);
};
