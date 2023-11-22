<?php

declare(strict_types=1);

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\Component\Logger;
use Keboola\DbExtractor\MSSQLApplication;

require __DIR__ . '/../vendor/autoload.php';

$handler = new \Monolog\Handler\StreamHandler('php://stderr', \Monolog\Logger::DEBUG);
$handler->setFormatter(new \Monolog\Formatter\LineFormatter("%message%\n"));
$logger = new Logger();
$logger->setHandlers([$handler]);
$logger->pushProcessor(function ($record) {
    $record['message'] = substr($record['message'],0,1024);
    return $record;
});

try {
    $app = new MSSQLApplication($logger);
    $app->execute();
    exit(0);
} catch (UserExceptionInterface $e) {
    $logger->error(substr($e->getMessage(), 0, 1024));
    exit(1);
} catch (Throwable $e) {
    $logger->critical(
        get_class($e) . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => is_object($e->getPrevious()) ? get_class($e->getPrevious()) : '',
        ],
    );
    exit(2);
}
