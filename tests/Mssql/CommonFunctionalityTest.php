<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Mssql\Tests;

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\MSSQLApplication;
use Keboola\DbExtractor\Test\AbstractExtractorTest;
use Keboola\DbExtractor\Test\DataLoaderInterface;

class CommonFunctionalityTest extends AbstractExtractorTest
{
    public const DRIVER = 'mssql';
    protected function getDataLoader(): DataLoaderInterface
    {
        $host = $this->getEnv(self::DRIVER, 'DB_HOST');
        $port = $this->getEnv(self::DRIVER, 'DB_PORT');
        $user = $this->getEnv(self::DRIVER, 'DB_USER');
        $pass = $this->getEnv(self::DRIVER, 'DB_PASSWORD');

        $db = new \PDO(
            sprintf('sqlsrv:Server=%s', $host),
            $user,
            $pass
        );
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->exec('USE master');
        return new MssqlDataLoader($db);
    }

    protected function getDbNameFromEnv(): string
    {
        return $this->getEnv(self::DRIVER, 'DB_DATABASE');
    }

    protected function getDataDir(): string
    {
        return __DIR__ . '/data';
    }

    protected function getApplication(array $config, array $state = []): Application
    {
        return new MSSQLApplication($config, new Logger(), $state, $this->getDataDir());
    }
}
