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
    $manager->generateAIRows('cdc_test_table');
    $manager->enableCdcOnTable('cdc_test_table');

    sleep(1);
    $lastFetchedTime = $manager->getServerTime();

    $connection->exec('UPDATE [cdc_test_table] SET [someInteger] = 10 WHERE [_Weir%d I-D] = 1');
    $manager->waitForCdcChanges('dbo_cdc_test_table', 2);

    // A new capture instance starts after the change the state points to, as when CDC retention
    // has already removed that change
    $manager->disableCdcOnTable('cdc_test_table');
    $manager->enableCdcOnTable('cdc_test_table');
    $connection->exec('UPDATE [cdc_test_table] SET [someInteger] = 20 WHERE [_Weir%d I-D] = 2');
    $manager->waitForCdcChanges('dbo_cdc_test_table', 2);

    (new Filesystem())->dumpFile(
        $test->testTempDir . '/in/state.json',
        (string) json_encode(['lastFetchedTime' => $lastFetchedTime]),
    );
};
