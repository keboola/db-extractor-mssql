<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\Exception as CsvException;
use PDOException;
use Keboola\DbExtractor\Exception\BcpAdapterException;
use Keboola\DbExtractor\Extractor\Adapters\BcpAdapter;
use Keboola\DbExtractor\Extractor\Adapters\PdoAdapter;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;

class MSSQL extends Extractor
{
    private MetadataProvider $metadataProvider;

    private PdoAdapter $pdoAdapter;

    private BcpAdapter $bcpAdapter;

    private QueryFactory $queryFactory;

    public static function getColumnMetadata(array $column): array
    {
        $datatype = new MssqlDataType(
            $column['type'],
            array_intersect_key($column, array_flip(MssqlDataType::DATATYPE_KEYS))
        );
        $columnMetadata = $datatype->toMetadata();
        $nonDatatypeKeys = array_diff_key($column, array_flip(MssqlDataType::DATATYPE_KEYS));
        foreach ($nonDatatypeKeys as $key => $value) {
            if ($key === 'name') {
                $columnMetadata[] = [
                    'key' => 'KBC.sourceName',
                    'value' => $value,
                ];
            } else {
                $columnMetadata[] = [
                    'key' => 'KBC.' . $key,
                    'value' => $value,
                ];
            }
        }
        return $columnMetadata;
    }

    public function createConnection(array $dbParams): void
    {
        $this->pdoAdapter = new PdoAdapter($this->logger, $dbParams, $this->state);
        $this->metadataProvider = new MetadataProvider($this->pdoAdapter);
        $this->bcpAdapter = new BcpAdapter(
            $this->logger,
            $this->pdoAdapter,
            $this->metadataProvider,
            $dbParams,
            $this->state
        );
        $this->queryFactory = new QueryFactory(
            $this->pdoAdapter,
            $this->metadataProvider,
            $this->state
        );
    }

    public function testConnection(): void
    {
        $this->pdoAdapter->testConnection();
    }

    public function getMaxOfIncrementalFetchingColumn(array $table): ?string
    {
        $maxTries = isset($table['retries']) ? (int) $table['retries'] : Extractor::DEFAULT_MAX_TRIES;
        $result = $this->pdoAdapter->runRetryableQuery(sprintf(
            $this->incrementalFetching['type'] === MssqlDataType::INCREMENT_TYPE_BINARY ?
                'SELECT CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), MAX(%s)), 1) %s FROM %s.%s' :
                'SELECT MAX(%s) %s FROM %s.%s',
            $this->pdoAdapter->quoteIdentifier($this->incrementalFetching['column']),
            $this->pdoAdapter->quoteIdentifier($this->incrementalFetching['column']),
            $this->pdoAdapter->quoteIdentifier($table['schema']),
            $this->pdoAdapter->quoteIdentifier($table['tableName'])
        ), $maxTries);

        return count($result) > 0 ? $result[0][$this->incrementalFetching['column']] : null;
    }

    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];
        $logPrefix = $outputTable;
        $this->logger->info('Exporting to ' . $outputTable);
        $isAdvancedQuery = array_key_exists('query', $table);
        $csvPath = $this->getOutputFilename($outputTable);

        // Fetch max value for incremental fetching without limit before execution
        $maxValue = $this->canFetchMaxIncrementalValueSeparately($isAdvancedQuery) ?
            $this->getMaxOfIncrementalFetchingColumn($table['table']) : null;

        // Create output dir, output CSV file is created in adapters
        $this->createOutputDir();

        // BCP adapter
        $result = null;
        if ($table['disableBcp']) {
            $this->logger->info('BCP export is disabled in the configuration.');
        } elseif ($isAdvancedQuery && $this->pdoAdapter->getServerVersion() < 11) {
            $this->logger->warning('BCP is not supported for advanced queries in sql server 2008 or less.');
        } else {
            $query = $this->queryFactory->create(
                $table,
                $this->incrementalFetching,
                QueryFactory::ESCAPING_TYPE_BCP
            );
            $this->logger->info(sprintf('Executing query "%s" via BCP: "%s"', $logPrefix, $query));

            try {
                $result = $this->bcpAdapter->export($table, $maxValue, $query, $this->incrementalFetching, $csvPath);
            } catch (BcpAdapterException $e) {
                @unlink($this->getOutputFilename($outputTable));
                $msg = sprintf('BCP export "%s" failed', $logPrefix);
                $msg .= $table['disableFallback'] ? ': ' : ' (will attempt via PDO): ';
                $msg .= $e->getMessage();
                $this->logger->info($msg);
            }
        }

        // PDO adapter
        if ($result === null) {
            if ($table['disableFallback']) {
                throw new UserException('BCP export failed and PDO fallback is disabled.');
            }

            $query = $this->queryFactory->create(
                $table,
                $this->incrementalFetching,
                QueryFactory::ESCAPING_TYPE_PDO
            );
            $this->logger->info(sprintf('Executing query "%s" via PDO: "%s"', $logPrefix, $query));

            try {
                $result = $this->pdoAdapter->export($table, $query, $this->incrementalFetching, $csvPath);
            } catch (CsvException $e) {
                throw new ApplicationException('Write to CSV failed: ' . $e->getMessage(), 0, $e);
            } catch (PDOException $e) {
                throw new UserException(
                    sprintf('PDO export "%s" failed: %s', $logPrefix, $e->getMessage()),
                    0,
                    $e
                );
            }
        }

        // Last fetched value
        if ($result['rows'] > 0) {
            if ($maxValue) {
                $result['lastFetchedRow'] = $maxValue;
            }
        } elseif (isset($this->incrementalFetching['column']) && isset($this->state['lastFetchedRow'])) {
            // No rows found.  If incremental fetching is turned on, we need to preserve the last state
            $result['lastFetchedRow'] = $this->state['lastFetchedRow'];
        }

        // Manifest
        if ($result['rows'] > 0) {
            $this->createManifest($table, $result['bcpColumns'] ?? null);
        } else {
            @unlink($this->getOutputFilename($outputTable));
            $this->logger->warning(sprintf(
                'Query "%s" returned empty result. Nothing was imported to "%s"',
                $logPrefix,
                $outputTable,
            ));
        }

        // Output state
        $output = [
            'outputTable' => $outputTable,
            'rows' => $result['rows'],
        ];

        if (isset($result['lastFetchedRow']) && !is_array($result['lastFetchedRow'])) {
            $output['state']['lastFetchedRow'] = $result['lastFetchedRow'];
        }

        return $output;
    }

    public function getTables(?array $tables = null): array
    {
        $proxy = new DbRetryProxy($this->logger);
        return $proxy->call(function () use ($tables): array {
            try {
                return $this->metadataProvider->getTables($tables);
            } catch (\Throwable $exception) {
                $this->pdoAdapter->tryReconnect();
                throw $exception;
            }
        });
    }

    public function simpleQuery(array $table, array $columns = []): string
    {
        throw new ApplicationException('This method is deprecated and should never get called');
    }

    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        $columns = $this->pdoAdapter->runRetryableQuery(sprintf(
            "SELECT [is_identity], TYPE_NAME([system_type_id]) AS [data_type]
            FROM [sys].[columns]
            WHERE [object_id] = OBJECT_ID('[%s].[%s]') AND [sys].[columns].[name] = '%s'",
            $table['schema'],
            $table['tableName'],
            $columnName
        ), self::DEFAULT_MAX_TRIES);

        if (count($columns) === 0) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $columnName
                )
            );
        }

        $this->incrementalFetching['column'] = $columnName;
        $this->incrementalFetching['type'] =
            MssqlDataType::getIncrementalFetchingType($columnName, $columns[0]['data_type']);

        if ($limit) {
            $this->incrementalFetching['limit'] = $limit;
        }
    }

    /**
     * @inheritDoc
     */
    protected function createManifest(array $table, ?array $bcpColumns = null)
    {
        parent::createManifest($table);

        // Output CSV file is generated without header when using BCP, so "columns" must be part of manifest files
        if ($bcpColumns) {
            $manifestFile = $this->getOutputFilename($table['outputTable']) . '.manifest';
            $columnsArray = $bcpColumns;
            $manifest = json_decode((string) file_get_contents($manifestFile), true);
            $manifest['columns'] = $columnsArray;
            file_put_contents($manifestFile, json_encode($manifest));
        }

        return true;
    }

    private function createOutputDir(): void
    {
        $outTablesDir = $this->dataDir . '/out/tables';
        if (!is_dir($outTablesDir)) {
            mkdir($outTablesDir, 0777, true);
        }
    }
}
