<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Symfony\Component\Filesystem\Filesystem;

return function (DatadirTest $test): void {
    $connection = $test->getConnection();
    $manager = new DatabaseManager($connection);
    $manager->resetCdcOnDatabase();

    $manager->createAITable('cdc_test_table');
    $manager->addAIConstraint('cdc_test_table');
    // Rows inserted before CDC is enabled stand for the data a previous run already exported
    $manager->generateAIRows('cdc_test_table');
    $manager->enableCdcOnTable('cdc_test_table');

    sleep(1);
    $lastFetchedTime = $manager->getServerTime();

    $connection->exec('UPDATE [cdc_test_table] SET [someInteger] = 20 WHERE [_Weir%d I-D] = 2');
    $connection->exec('DELETE FROM [cdc_test_table] WHERE [_Weir%d I-D] = 3');
    $manager->insertRows(
        'cdc_test_table',
        ['Weir%d Na-me', 'type', 'someInteger', 'someDecimal', 'smalldatetime', 'datetime'],
        [['peach', 'royalty', 7, 7.7, '2023-08-10 10:25', '2023-08-10 13:43:27.123']],
    );
    // An update writes two rows to the change table, one before and one after the change
    $manager->waitForCdcChanges('dbo_cdc_test_table', 4);

    (new Filesystem())->dumpFile(
        $test->testTempDir . '/in/state.json',
        (string) json_encode(['lastFetchedTime' => $lastFetchedTime]),
    );
};
