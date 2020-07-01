<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor\Adapters;

use PDO;
use PDOException;
use Throwable;
use Retry\RetryProxy;
use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Extractor\Extractor;
use Keboola\DbExtractorLogger\Logger;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;

class PdoAdapter
{
    private Logger $logger;

    private PDO $pdo;

    private array $dbParams;

    private array $state;

    private ?int $serverVersion = null;

    public function __construct(Logger $logger, array $dbParams, array $state)
    {
        if (isset($dbParams['#password'])) {
            $dbParams['password'] = $dbParams['#password'];
        }

        $this->logger = $logger;
        $this->dbParams = $dbParams;
        $this->state = $state;

        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!array_key_exists($r, $dbParams)) {
                throw new UserException(sprintf('Parameter %s is missing.', $r));
            }
        }

        $this->createConnection();
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

    public function getServerVersion(): int
    {
        if (!$this->serverVersion) {
            $this->serverVersion = $this->fetchServerVersion();
        }
        return $this->serverVersion;
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

    public function export(array $table, string $query, ?array $incrementalFetching, string $csvPath): array
    {
        $isAdvancedQuery = array_key_exists('query', $table);
        $maxTries = isset($table['retries']) ? (int) $table['retries'] : Extractor::DEFAULT_MAX_TRIES;

        // Check connection
        $this->tryReconnect();

        $result =  $this
            ->createRetryProxy($maxTries)
            ->call(function () use ($query, $isAdvancedQuery, $incrementalFetching, $csvPath) {
                try {
                    // Csv writer must be re-created after each error, because some lines could be already written
                    $csv = new CsvFile($csvPath);
                    $result =  $this->executeAndWrite($query, $isAdvancedQuery, $incrementalFetching, $csv);
                    $this->isAlive();
                    return $result;
                } catch (Throwable $queryError) {
                    try {
                        $this->createConnection();
                    } catch (Throwable $connectionError) {
                    };
                    throw $queryError;
                }
            });

        return $result;
    }

    private function executeAndWrite(
        string $query,
        bool $includeHeader,
        ?array $incrementalFetching,
        CsvFile $csv
    ): array {
        $stmt = $this->pdo->prepare($query);
        $stmt->execute();

        $output = [];

        $resultRow = $stmt->fetch(PDO::FETCH_ASSOC);

        if (is_array($resultRow) && !empty($resultRow)) {
            // write header and first line
            if ($includeHeader) {
                $csv->writeRow(array_keys($resultRow));
            }
            $csv->writeRow($resultRow);

            // write the rest
            $numRows = 1;
            $lastRow = $resultRow;

            while ($resultRow = $stmt->fetch(PDO::FETCH_ASSOC)) {
                $csv->writeRow($resultRow);
                $lastRow = $resultRow;
                $numRows++;
            }
            $stmt->closeCursor();

            if (isset($incrementalFetching['column'])) {
                if (!array_key_exists($incrementalFetching['column'], $lastRow)) {
                    throw new UserException(
                        sprintf(
                            'The specified incremental fetching column %s not found in the table',
                            $incrementalFetching['column']
                        )
                    );
                }
                $output['lastFetchedRow'] = $lastRow[$incrementalFetching['column']];
            }
            $output['rows'] = $numRows;
            return $output;
        }

        $output['rows'] = 0;
        return $output;
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
                    $this->createConnection();
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

    private function createConnection(): void
    {
        $host = $this->dbParams['host'];
        $host .= (isset($this->dbParams['port']) && $this->dbParams['port'] !== '1433') ?
            ',' . $this->dbParams['port'] : '';
        $host .= empty($this->dbParams['instance']) ? '' : '\\\\' . $this->dbParams['instance'];
        $options[] = 'Server=' . $host;
        $options[] = 'Database=' . $this->dbParams['database'];
        $dsn = sprintf('sqlsrv:%s', implode(';', $options));
        $this->logger->info("Connecting to DSN '" . $dsn . "'");

        // ms sql doesn't support options
        $this->pdo = new PDO($dsn, $this->dbParams['user'], $this->dbParams['password']);
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
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

    private function isAlive(): void
    {
        try {
            $this->testConnection();
        } catch (\Throwable $e) {
            throw new DeadConnectionException('Dead connection: ' . $e->getMessage());
        }
    }

    private function createRetryProxy(int $maxTries): RetryProxy
    {
        return new DbRetryProxy($this->logger, $maxTries, [PDOException::class]);
    }
}