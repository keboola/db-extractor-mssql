<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Symfony\Component\Filesystem\Filesystem;

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

    // write state file
    $fs = new Filesystem();
    $fs->mkdir($test->testProjectDir . '/source/data/in/');

    sleep(6);

    $sqlToLsnTime = <<<SQL
DECLARE @to_lsn binary(10);
SET @to_lsn = [sys].[fn_cdc_get_max_lsn]();
SELECT sys.fn_cdc_map_lsn_to_time(@to_lsn) as last_fetched_time;
SQL;
    $sqlToLsnTime = $test->getConnection()->query($sqlToLsnTime);
    if ($sqlToLsnTime instanceof PDOStatement) {
        $lsnTimeResponse = (array) $sqlToLsnTime->fetchAll();
        assert(count($lsnTimeResponse) === 1, 'Expected one row');
        $lsnTime = $lsnTimeResponse[0]['last_fetched_time'];

        file_put_contents(
            $test->testProjectDir . '/source/data/in/state.json',
            json_encode(['lastFetchedTime' => $lsnTime]),
        );
    }

    $newData = [
        'columns' => ['Weir%d Na-me', 'type', 'someInteger', 'someDecimal', 'smalldatetime', 'datetime'],
        'data' => [
            ['nedData', 'horse?', 6, 6.6, '2023-08-10 10:25', '2023-08-10 13:43:27.123'],
            ['asd', 'horse?', 6, 6.6, '2023-08-10 10:25', '2023-08-10 13:43:27.123'],
        ],
    ];
    $manager->insertRows('cdc_test_table', $newData['columns'], $newData['data']);

    sleep(15);
};
