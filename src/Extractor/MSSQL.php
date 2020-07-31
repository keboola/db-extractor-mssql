<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Component\JsonHelper;
use Keboola\DbExtractor\Metadata\MssqlMetadataProvider;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use PDOException;
use InvalidArgumentException;
use Keboola\Csv\Exception as CsvException;
use Keboola\DbExtractor\TableResultFormat\Exception\ColumnNotFoundException;
use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractor\Metadata\MssqlManifestSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\ManifestSerializer;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\Exception\BcpAdapterException;
use Keboola\DbExtractor\Extractor\Adapters\BcpAdapter;
use Keboola\DbExtractor\Extractor\Adapters\PdoAdapter;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;

class MSSQL extends BaseExtractor
{
    private MetadataProvider $metadataProvider;

    private PdoConnection $pdo;

    private PdoAdapter $pdoAdapter;

    private BcpAdapter $bcpAdapter;

    private QueryFactory $queryFactory;

    private ?string $incrementalFetchingType = null;

    public function getMetadataProvider(): MetadataProvider
    {
        return $this->metadataProvider;
    }

    public function getManifestMetadataSerializer(): ManifestSerializer
    {
        return new MssqlManifestSerializer();
    }

    public function createConnection(DatabaseConfig $databaseConfig): void
    {
        if ($databaseConfig->hasSSLConnection()) {
            if ($databaseConfig->getSslConnectionConfig()->hasCipher()) {
                $changed = $this->saveSslCipherString($databaseConfig);

                // HACK: OpenSSL config is changed -> process must be reloaded.
                // So run self, then will be $changed = false, and execution will be continue
                if ($changed) {
                    $this->logger->info('OpenSSL configuration was updated. Running process again.');
                    passthru(PHP_BINARY . ' ' . $_SERVER['SCRIPT_FILENAME'], $exitCode);
                    exit($exitCode);
                }
            }
            if ($databaseConfig->getSslConnectionConfig()->isVerifyServerCert()) {
                $this->saveSslCertificate($databaseConfig);
            }
        }
        $this->pdo = new PdoConnection($this->logger, $databaseConfig);
        $this->pdoAdapter = new PdoAdapter($this->logger, $this->pdo, $this->state);
        $this->metadataProvider = new MssqlMetadataProvider($this->pdo);
        $this->bcpAdapter = new BcpAdapter(
            $this->logger,
            $this->pdo,
            $this->metadataProvider,
            $databaseConfig,
            $this->state
        );
        $this->queryFactory = new QueryFactory(
            $this->pdo,
            $this->metadataProvider,
            $this->state
        );
    }

    public function testConnection(): void
    {
        $this->pdo->testConnection();
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        $result = $this->pdo->runRetryableQuery(sprintf(
            $this->incrementalFetchingType === MssqlDataType::INCREMENT_TYPE_BINARY ?
                'SELECT CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), MAX(%s)), 1) %s FROM %s.%s' :
                'SELECT MAX(%s) %s FROM %s.%s',
            $this->pdo->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->pdo->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->pdo->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->pdo->quoteIdentifier($exportConfig->getTable()->getName())
        ), $exportConfig->getMaxRetries());

        return count($result) > 0 ? $result[0][$exportConfig->getIncrementalFetchingColumn()] : null;
    }

    public function export(ExportConfig $exportConfig): array
    {
        if (!$exportConfig instanceof MssqlExportConfig) {
            throw new InvalidArgumentException('MssqlExportConfig expected.');
        }

        $logPrefix = $exportConfig->hasConfigName() ? $exportConfig->getConfigName() : $exportConfig->getOutputTable();
        $this->logger->info('Exporting to ' . $exportConfig->getOutputTable());
        $csvPath = $this->getOutputFilename($exportConfig->getOutputTable());

        // Fetch max value for incremental fetching without limit before execution
        if ($exportConfig->isIncrementalFetching()) {
            $this->validateIncrementalFetching($exportConfig);
            $maxValue = $this->canFetchMaxIncrementalValueSeparately($exportConfig) ?
                $this->getMaxOfIncrementalFetchingColumn($exportConfig) : null;
        } else {
            $maxValue = null;
        }

        // Create output dir, output CSV file is created in adapters
        $this->createOutputDir();

        // BCP adapter
        $result = null;
        if ($exportConfig->isBcpDisabled()) {
            $this->logger->info('BCP export is disabled in the configuration.');
        } elseif ($exportConfig->hasQuery() && $this->pdo->getServerVersion() < 11) {
            $this->logger->warning('BCP is not supported for advanced queries in sql server 2008 or less.');
        } else {
            $query = $this->queryFactory->create(
                $exportConfig,
                QueryFactory::ESCAPING_TYPE_BCP,
                $this->incrementalFetchingType
            );
            $this->logger->info(sprintf('Executing query "%s" via BCP: "%s"', $logPrefix, $query));

            try {
                $result = $this->bcpAdapter->export(
                    $exportConfig,
                    $maxValue,
                    $query,
                    $csvPath,
                    $this->incrementalFetchingType,
                );
            } catch (BcpAdapterException $e) {
                @unlink($this->getOutputFilename($exportConfig->getOutputTable()));
                $context = $e->getData();
                $msg = sprintf('BCP export "%s" failed', $logPrefix);
                $msg .= $exportConfig->isFallbackDisabled() ? ': ' : ' (will attempt via PDO): ';
                $msg .= $e->getMessage();
                $msg .= $context ? ', context: ' . JsonHelper::encode($context) : '';
                $this->logger->info($msg);
            }
        }

        // PDO adapter
        if ($result === null) {
            if ($exportConfig->isFallbackDisabled()) {
                throw new UserException('BCP export failed and PDO fallback is disabled.');
            }

            $query = $this->queryFactory->create(
                $exportConfig,
                QueryFactory::ESCAPING_TYPE_PDO,
                $this->incrementalFetchingType
            );
            $this->logger->info(sprintf('Executing query "%s" via PDO: "%s"', $logPrefix, $query));

            try {
                $result = $this->pdoAdapter->export($exportConfig, $query, $csvPath);
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
        } elseif ($exportConfig->isIncrementalFetching()&& isset($this->state['lastFetchedRow'])) {
            // No rows found.  If incremental fetching is turned on, we need to preserve the last state
            $result['lastFetchedRow'] = $this->state['lastFetchedRow'];
        }

        // Manifest
        if ($result['rows'] > 0) {
            $this->createManifest($exportConfig, $result['bcpColumns'] ?? null);
        } else {
            @unlink($this->getOutputFilename($exportConfig->getOutputTable()));
            $this->logger->warning(sprintf(
                'Query "%s" returned empty result. Nothing was imported to "%s"',
                $logPrefix,
                $exportConfig->getOutputTable(),
            ));
        }

        // Output state
        $output = [
            'outputTable' => $exportConfig->getOutputTable(),
            'rows' => $result['rows'],
        ];

        if (isset($result['lastFetchedRow']) && !is_array($result['lastFetchedRow'])) {
            $output['state']['lastFetchedRow'] = $result['lastFetchedRow'];
        }

        return $output;
    }

    public function simpleQuery(ExportConfig $exportConfig): string
    {
        throw new ApplicationException('This method is deprecated and should never get called');
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        try {
            $column = $this->metadataProvider
               ->getTable($exportConfig->getTable())
               ->getColumns()
               ->getByName($exportConfig->getIncrementalFetchingColumn());
        } catch (ColumnNotFoundException $e) {
            throw new ColumnNotFoundException(sprintf(
                'Column "%s" specified for incremental fetching was not found.',
                $exportConfig->getIncrementalFetchingColumn(),
            ), 0, $e);
        }

        $this->incrementalFetchingType =
            MssqlDataType::getIncrementalFetchingType($column->getName(), $column->getType());
    }

    protected function createManifest(ExportConfig $exportConfig, ?array $bcpColumns = null): void
    {
        parent::createManifest($exportConfig);

        // Output CSV file is generated without header when using BCP, so "columns" must be part of manifest files
        if ($bcpColumns) {
            $manifestFile = $this->getOutputFilename($exportConfig->getOutputTable()) . '.manifest';
            $manifest = json_decode((string) file_get_contents($manifestFile), true);
            $manifest['columns'] = $bcpColumns;
            file_put_contents($manifestFile, json_encode($manifest));
        }
    }

    private function createOutputDir(): void
    {
        $outTablesDir = $this->dataDir . '/out/tables';
        if (!is_dir($outTablesDir)) {
            mkdir($outTablesDir, 0777, true);
        }
    }

    private function saveSslCertificate(DatabaseConfig $databaseConfig): void
    {
        file_put_contents(
            '/usr/local/share/ca-certificates/mssql.crt',
            $databaseConfig->getSslConnectionConfig()->getCa()
        );
        Process::fromShellCommandline('update-ca-certificates')->mustRun();
    }

    private function saveSslCipherString(DatabaseConfig $databaseConfig): bool
    {
        $confFile = '/etc/ssl/openssl.cnf';
        $cipherString = str_ireplace(
            ["\r", "\n"],
            ' ',
            $databaseConfig->getSslConnectionConfig()->getCipher()
        );

        $oldContent = file_get_contents($confFile);
        Process::fromShellCommandline(
            sprintf(
                "sed -i 's/CipherString\s*=.*/CipherString = %s/g' %s",
                $cipherString,
                $confFile
            )
        )->mustRun();
        $newContent = file_get_contents($confFile);

        return $oldContent !== $newContent;
    }
}
