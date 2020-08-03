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
        $options[] = 'Server=' . $host;
        $options[] = 'Database=' . $this->databaseConfig->getDatabase();
        $dsn = sprintf('sqlsrv:%s', implode(';', $options));
        $this->logger->info("Connecting to DSN '" . $dsn . "'");

        // ms sql doesn't support options
        $this->pdo = new PDO($dsn, $this->databaseConfig->getUsername(), $this->databaseConfig->getPassword());
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    private function runQuery(string $query, array $values = []): array
    {
        $stmt = $this->pdo->prepare($query);
        $stmt->execute($values);
        /** @var array $result */
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        return $result;
    }
}
