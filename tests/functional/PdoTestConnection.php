<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use PDO;
use PDOException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;

class PdoTestConnection
{
    public static function getDbConfigArray(): array
    {
        return [
            'host' => (string) getenv('MSSQL_DB_HOST'),
            'port' => (string) getenv('MSSQL_DB_PORT'),
            'user' => (string) getenv('MSSQL_DB_USER'),
            '#password' => (string) getenv('MSSQL_DB_PASSWORD'),
            'database' => (string) getenv('MSSQL_DB_DATABASE'),
        ];
    }

    public static function createDbConfig(?string $dbHost = null): DatabaseConfig
    {
        $dbConfig = self::getDbConfigArray();
        if ($dbHost) {
            $dbConfig['host'] = $dbHost;
        }
        return DatabaseConfig::fromArray($dbConfig);
    }

    public static function createConnection(?string $dbHost = null): PDO
    {
        $dbConfig = self::createDbConfig($dbHost);

        $host = $dbConfig->getHost();
        $host .= $dbConfig->hasPort() ? ',' . $dbConfig->getPort() : '';
        $dsn['Server'] = $host;
        if ($dbConfig->hasSSLConnection()) {
            $dsn['Encrypt'] = 'true';
            $dsn['TrustServerCertificate'] =
                $dbConfig->getSslConnectionConfig()->isVerifyServerCert() ? 'false' : 'true';
        }

        // ms sql doesn't support options
        try {
            $pdo = self::createPdoInstance($dbConfig, $dsn);
        } catch (PDOException $e) {
            if (strpos($e->getMessage(), 'certificate verify failed:subject name does not match host name') &&
                $dbConfig->hasSSLConnection() &&
                $dbConfig->getSslConnectionConfig()->isIgnoreCertificateCn()
            ) {
                $dsn['TrustServerCertificate'] = 'true';

                $pdo = self::createPdoInstance($dbConfig, $dsn);
            } else {
                throw $e;
            }
        }

        $pdo->exec('USE master');
        $pdo->exec(sprintf("
            IF NOT EXISTS(select * from sys.databases where name='%s') 
            CREATE DATABASE %s
        ", $dbConfig->getDatabase(), $dbConfig->getDatabase()));
        $pdo->exec(sprintf('USE %s', $dbConfig->getDatabase()));

        return $pdo;
    }

    private static function createPdoInstance(DatabaseConfig $dbConfig, array $dsn): PDO
    {
        $dsn = sprintf('sqlsrv:%s', implode(';', array_map(function ($key, $item) {
            return sprintf('%s=%s', $key, $item);
        }, array_keys($dsn), $dsn)));

        return new PDO($dsn, $dbConfig->getUsername(), $dbConfig->getPassword(), [
            PDO::ATTR_ERRMODE  => PDO::ERRMODE_EXCEPTION,
        ]);
    }
}
