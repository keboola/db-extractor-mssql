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

    // Without a capture run there is no LSN to map a time to, as when the capture job has never run
    $manager->removeCdcCaptureJob();
    sleep(2);
    $connection->exec('DELETE FROM [cdc].[lsn_time_mapping];');

    $lastFetchedTime = $manager->getServerTime();
    $connection->exec('UPDATE [cdc_test_table] SET [someInteger] = 10 WHERE [_Weir%d I-D] = 1');

    (new Filesystem())->dumpFile(
        $test->testTempDir . '/in/state.json',
        (string) json_encode(['lastFetchedTime' => $lastFetchedTime]),
    );
};
