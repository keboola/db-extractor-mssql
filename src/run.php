<?php

declare(strict_types=1);

use Keboola\DbExtractor\MSSQLApplication;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Yaml\Yaml;

require_once(dirname(__FILE__) . "/../vendor/autoload.php");

$logger = new Logger('ex-db-mssql');
$runAction = true;

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    if (file_exists($arguments["data"] . "/config.yml")) {
        $config = Yaml::parse(
            file_get_contents($arguments["data"] . "/config.yml")
        );
    } else if (file_exists($arguments["data"] . "/config.json")) {
        $config = json_decode(
            file_get_contents($arguments["data"] . '/config.json'),
            true
        );
    } else {
        throw new UserException('Configuration file not found.');
    }

    // get the state
    $inputState = [];
    $inputStateFile = $arguments['data'] . '/in/state.json';
    if (file_exists($inputStateFile)) {
        $inputState = json_decode(file_get_contents($inputStateFile), true);
    }

    $app = new MSSQLApplication(
        $config,
        $logger,
        $inputState,
        $arguments["data"]
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
            $outputStateFile = $arguments['data'] . '/out/state.json';
            file_put_contents($outputStateFile, json_encode($result['state'], true));
        }
    }

    $app['logger']->log('info', "Extractor finished successfully.");
    exit(0);
} catch (UserException $e) {
    $logger->log('error', $e->getMessage(), $e->getData());
    if (!$runAction) {
        echo $e->getMessage();
    }
    exit(1);
} catch (ApplicationException $e) {
    $logger->log('error', $e->getMessage(), array_merge(
        $e->getData(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'trace' => $e->getTrace(),
        ]
    ));
    exit($e->getCode() > 1 ? $e->getCode(): 2);
} catch (\Throwable $e) {
    $logger->log(
        'error',
        $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'trace' => $e->getTrace(),
        ]
    );
    exit(2);
}
