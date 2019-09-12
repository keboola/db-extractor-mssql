<?php

declare(strict_types=1);

use Keboola\DbExtractor\MSSQLApplication;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Yaml\Yaml;

require_once(dirname(__FILE__) . '/../vendor/autoload.php');

$logger = new Logger('ex-db-mssql');
$runAction = true;

try {
    $arguments = getopt('d::', ['data::']);
    if (!isset($arguments['data']) || !is_string($arguments['data'])) {
        throw new UserException('Data folder not set.');
    }
    $dataFolder = $arguments['data'];

    if (file_exists($dataFolder . '/config.yml')) {
        $config = Yaml::parse(
            (string) file_get_contents($dataFolder . '/config.yml')
        );
    } else if (file_exists($dataFolder . '/config.json')) {
        $config = json_decode(
            (string) file_get_contents($dataFolder . '/config.json'),
            true
        );
    } else {
        throw new UserException('Configuration file not found.');
    }

    // get the state
    $inputState = [];
    $inputStateFile = $dataFolder . '/in/state.json';
    if (file_exists($inputStateFile)) {
        $inputState = json_decode((string) file_get_contents($inputStateFile), true);
    }

    $app = new MSSQLApplication(
        $config,
        $logger,
        $inputState,
        $dataFolder
    );

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
        $runAction = false;
    }

    $result = $app->run();

    if (!$runAction) {
        echo json_encode($result);
    } else {
        if (!empty($result['state'])) {
            // write state
            $outputStateFile = $dataFolder . '/out/state.json';
            file_put_contents($outputStateFile, json_encode($result['state']));
        }
    }

    $app['logger']->log('info', 'Extractor finished successfully.');
    exit(0);
} catch (UserException $e) {
    $logger->log('error', $e->getMessage());
    if (!$runAction) {
        echo $e->getMessage();
    }
    exit(1);
} catch (\Throwable $e) {
    $logger->critical(
        get_class($e) . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => is_object($e->getPrevious()) ? get_class($e->getPrevious()) : '',
        ]
    );
    exit(2);
}
