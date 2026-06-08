<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

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
            // Regression for SUPPORT-16443: a NULL in the nullable "smalldatetime" column must be
            // exported as an unquoted empty field. With the default CsvWriter (pre-fix) the PDO
            // adapter forced by CDC mode wrote it as "", which Snowflake cannot load into a typed
            // timestamp column. Without the NullAwareCsvWriter fix this fixture fails.
            ['nullDate', 'tester', 7, 7.7, null, '2023-08-10 13:43:27.123'],
        ],
    ];
    $manager->insertRows('cdc_test_table', $newData['columns'], $newData['data']);

    sleep(6);
};
