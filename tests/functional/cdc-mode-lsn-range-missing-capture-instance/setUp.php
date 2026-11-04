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

    // CDC runs on another table only, so the database has LSNs but no capture instance for this table
    $manager->createTable('cdc_other_table', ['id' => 'INT NOT NULL PRIMARY KEY', 'name' => 'VARCHAR(55)']);
    $manager->enableCdcOnTable('cdc_other_table');
    $connection->exec("INSERT INTO [cdc_other_table] ([id], [name]) VALUES (1, 'other')");
    $manager->waitForCdcChanges('dbo_cdc_other_table', 1);

    $lastFetchedTime = $manager->getServerTime();

    (new Filesystem())->dumpFile(
        $test->testTempDir . '/in/state.json',
        (string) json_encode(['lastFetchedTime' => $lastFetchedTime]),
    );
};
