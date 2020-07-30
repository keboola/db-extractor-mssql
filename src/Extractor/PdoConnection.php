<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use PDO;
use PDOStatement;
use Psr\Log\LoggerInterface;
use Throwable;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;
use \PDOException;

class PdoConnection
{
    private LoggerInterface $logger;

    private PDO $pdo;

    private DatabaseConfig $databaseConfig;

    private ?int $serverVersion = null;

    public function __construct(LoggerInterface $logger, DatabaseConfig $databaseConfig)
    {
        $this->logger = $logger;
        $this->databaseConfig = $databaseConfig;

        $this->connect();
    }

    public function testConnection(): void
    {
        $this->pdo->query('SELECT GETDATE() AS CurrentDateTime')->execute();
    }

    public function quote(string $str): string
    {
        return $this->pdo->quote($str);
    }

    public function quoteIdentifier(string $str): string
    {
        return "[{$str}]";
    }

    public function prepare(string $statement, array $driver_options = array()): PDOStatement
    {
        return $this->pdo->prepare($statement, $driver_options);
    }

    public function getServerVersion(): int
    {
        if (!$this->serverVersion) {
            $this->serverVersion = $this->fetchServerVersion();
        }
        return $this->serverVersion;
    }

    private function fetchServerVersion(): int
    {
        // Get the MSSQL Server version (note, 2008 is version 10.*)
        $res = $this->pdo->query("SELECT SERVERPROPERTY('ProductVersion') AS version;");

        $versionResult = $res->fetch(\PDO::FETCH_ASSOC);
        if (!isset($versionResult['version'])) {
            throw new UserException('Unable to get SQL Server Version Information');
        }

        $versionString = $versionResult['version'];
        $versionParts = explode('.', $versionString);
        $this->logger->info(sprintf('Found database server version: %s', $versionString));

        return (int) $versionParts[0];
    }

    public function runQuery(string $query, array $values = []): array
    {
        $stmt = $this->pdo->prepare($query);
        $stmt->execute($values);
        /** @var array $result */
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        return $result;
    }

    public function runRetryableQuery(string $query, int $maxTries, array $values = []): array
    {
        $retryProxy = new DbRetryProxy($this->logger, $maxTries);
        return $retryProxy->call(function () use ($query, $values): array {
            try {
                return $this->runQuery($query, $values);
            } catch (\Throwable $exception) {
                $this->tryReconnect();
                throw $exception;
            }
        });
    }

    public function tryReconnect(): void
    {
        try {
            $this->isAlive();
        } catch (DeadConnectionException $e) {
            $reconnectionRetryProxy = new DbRetryProxy(
                $this->logger,
                DbRetryProxy::DEFAULT_MAX_TRIES,
                null,
                1000
            );
            try {
                $reconnectionRetryProxy->call(function (): void {
                    $this->connect();
                });
            } catch (Throwable $reconnectException) {
                throw new UserException(
                    'Unable to reconnect to the database: ' . $reconnectException->getMessage(),
                    $reconnectException->getCode(),
                    $reconnectException
                );
            }
        }
    }

    public function isAlive(): void
    {
        try {
            $this->testConnection();
        } catch (\Throwable $e) {
            throw new DeadConnectionException('Dead connection: ' . $e->getMessage());
        }
    }

    public function connect(): void
    {
        $host = $this->databaseConfig->getHost();
        $host .= $this->databaseConfig->hasPort() ? ',' . $this->databaseConfig->getPort() : '';
        $options['Server'] = $host;
        $options['Database'] = $this->databaseConfig->getDatabase();
        if ($this->databaseConfig->hasSSLConnection()) {
            $options['Encrypt'] = 'true';
            $options['TrustServerCertificate'] =
                $this->databaseConfig->getSslConnectionConfig()->isVerifyServerCert() ? 'false' : 'true';
        }
        $dsn = sprintf('sqlsrv:%s', implode(';', array_map(function ($key, $item) {
            return sprintf('%s=%s', $key, $item);
        }, array_keys($options), $options)));

        $this->logger->info("Connecting to DSN '" . $dsn . "'");

        // ms sql doesn't support options
        try {
            $this->pdo = new PDO($dsn, $this->databaseConfig->getUsername(), $this->databaseConfig->getPassword());
        } catch (PDOException $e) {
            if (strpos($e->getMessage(), 'certificate verify failed:subject name does not match host name') &&
                $this->databaseConfig->hasSSLConnection() &&
                $this->databaseConfig->getSslConnectionConfig()->isIgnoreCertificateCn()
            ) {
                $this->logger->warning($e->getMessage());

                $options['TrustServerCertificate'] = 'true';

                $dsn = sprintf('sqlsrv:%s', implode(';', array_map(function ($key, $item) {
                    return sprintf('%s=%s', $key, $item);
                }, array_keys($options), $options)));

                $this->pdo = new PDO($dsn, $this->databaseConfig->getUsername(), $this->databaseConfig->getPassword());
            } else {
                throw $e;
            }
        }
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        if ($this->databaseConfig->hasSSLConnection()) {
            $status = $this->pdo->query(
                'SELECT session_id, encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID'
            )->fetch();
            if ($status['encrypt_option'] === 'FALSE') {
                throw new UserException(sprintf('Connection is not encrypted'));
            } else {
                $this->logger->info('Using SSL connection');
            }
        }
    }
}
