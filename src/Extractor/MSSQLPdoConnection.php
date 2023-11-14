<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\Exception\DeadConnectionException;
use Keboola\DbExtractor\Adapter\PDO\PdoConnection;
use Keboola\DbExtractor\Adapter\PDO\PdoQueryResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use PDO;
use PDOException;
use PDOStatement;
use Psr\Log\LoggerInterface;
use Throwable;

class MSSQLPdoConnection extends PdoConnection
{
    private DatabaseConfig $databaseConfig;

    private ?int $serverVersion = null;

    public function __construct(
        LoggerInterface $logger,
        DatabaseConfig $databaseConfig,
        int $connectMaxRetries = self::CONNECT_DEFAULT_MAX_RETRIES,
    ) {
        $this->logger = $logger;
        $this->databaseConfig = $databaseConfig;

        $this->connectMaxRetries = max($connectMaxRetries, 1);
        $this->connectWithRetry();
    }

    public function testConnection(): void
    {
        $this->pdo->query('SELECT GETDATE() AS CurrentDateTime')->execute();
    }

    public function quoteIdentifier(string $str): string
    {
        return "[{$str}]";
    }

    public function prepare(string $statement, array $driver_options = []): PDOStatement
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

        $versionResult = $res->fetch(PDO::FETCH_ASSOC);
        if (!isset($versionResult['version'])) {
            throw new UserException('Unable to get SQL Server Version Information');
        }

        $versionString = $versionResult['version'];
        $versionParts = explode('.', $versionString);
        $this->logger->info(sprintf('Found database server version: %s', $versionString));

        return (int) $versionParts[0];
    }

    public function tryReconnect(): void
    {
        try {
            $this->isAlive();
        } catch (DeadConnectionException $e) {
            $reconnectionRetryProxy = MssqlRetryFactory::createProxy($this->logger, DbRetryProxy::DEFAULT_MAX_TRIES);
            try {
                $reconnectionRetryProxy->call(function (): void {
                    $this->connect();
                });
            } catch (Throwable $reconnectException) {
                throw new UserException(
                    'Unable to reconnect to the database: ' . $reconnectException->getMessage(),
                    (int) $reconnectException->getCode(),
                    $reconnectException,
                );
            }
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

        // ms sql doesn't support options
        try {
            $this->pdo = $this->createPdoInstance($options);
        } catch (PDOException $e) {
            if (strpos($e->getMessage(), 'certificate verify failed:subject name does not match host name') &&
                $this->databaseConfig->hasSSLConnection() &&
                $this->databaseConfig->getSslConnectionConfig()->isIgnoreCertificateCn()
            ) {
                $this->logger->warning($e->getMessage());

                $options['TrustServerCertificate'] = 'true';

                $this->pdo = $this->createPdoInstance($options);
            } else {
                throw new UserException($e->getMessage(), 0, $e);
            }
        }
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        if ($this->databaseConfig->hasSSLConnection()) {
            $status = $this->pdo->query(
                'SELECT session_id, encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID',
            )->fetch();
            if ($status['encrypt_option'] === 'FALSE') {
                throw new UserException(sprintf('Connection is not encrypted'));
            } else {
                $this->logger->info('Using SSL connection');
            }
        }
    }

    private function createPdoInstance(array $options): PDO
    {
        $dsn = sprintf('sqlsrv:%s', implode(';', array_map(function ($key, $item) {
            return sprintf('%s=%s', $key, $item);
        }, array_keys($options), $options)));

        $this->logger->info("Connecting to DSN '" . $dsn . "'");

        return new PDO($dsn, $this->databaseConfig->getUsername(), $this->databaseConfig->getPassword());
    }

    public function query(string $query, int $maxRetries = self::DEFAULT_MAX_RETRIES, array $values = []): QueryResult
    {
        return $this->callWithRetry(
            $maxRetries,
            function () use ($query, $values) {
                return $this->queryReconnectOnError($query, $values);
            },
        );
    }

    protected function queryReconnectOnError(string $query, array $values = []): QueryResult
    {
        try {
            return $this->doQuery($query, $values);
        } catch (Throwable $e) {
            try {
                // Reconnect
                $this->connect();
            } catch (Throwable $e) {
            }
            throw $e;
        }
    }

    protected function doQuery(string $query, array $values = []): QueryResult
    {
        /** @var PDOStatement $stmt */
        $stmt = $this->pdo->prepare($query);
        $stmt->execute($values);
        $queryMetadata = $this->getQueryMetadata($query, $stmt);
        return new PdoQueryResult($query, $queryMetadata, $stmt);
    }
}
