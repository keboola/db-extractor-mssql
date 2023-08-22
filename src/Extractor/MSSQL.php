<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\FallbackExportAdapter;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractor\Extractor\Adapters\BcpExportAdapter;
use Keboola\DbExtractor\Manifest\DefaultManifestGenerator;
use Keboola\DbExtractor\Manifest\ManifestGenerator;
use Keboola\DbExtractor\Metadata\MssqlMetadataProvider;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractor\TableResultFormat\Exception\ColumnNotFoundException;
use Keboola\DbExtractor\Metadata\MssqlManifestSerializer;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\Extractor\Adapters\MSSQLPdoExportAdapter;
use Symfony\Component\Process\Process;
use Throwable;

class MSSQL extends BaseExtractor
{
    protected MetadataProvider $metadataProvider;

    private MSSQLPdoConnection $connection;

    private ?MSSQLQueryFactory $queryFactory = null;

    public function createMetadataProvider(): MssqlMetadataProvider
    {
        return new MssqlMetadataProvider($this->connection);
    }

    protected function createManifestGenerator(): ManifestGenerator
    {
        return new DefaultManifestGenerator(
            $this->getMetadataProvider(),
            new MssqlManifestSerializer()
        );
    }

    public function getQueryFactory(): MSSQLQueryFactory
    {
        if (!$this->queryFactory) {
            $this->queryFactory = new MSSQLQueryFactory(
                $this->state,
                $this->createMetadataProvider()
            );
        }
        return $this->queryFactory;
    }

    protected function createExportAdapter(): ExportAdapter
    {
        $adapters = [];

        $adapters[] = new BcpExportAdapter(
            $this->logger,
            $this->connection,
            $this->createMetadataProvider(),
            $this->getDatabaseConfig(),
            $this->getQueryFactory()
        );

        $adapters[] = new MSSQLPdoExportAdapter(
            $this->logger,
            $this->connection,
            $this->getQueryFactory(),
            new MSSQLResultWriter($this->state),
            $this->dataDir,
            $this->state
        );

        return new FallbackExportAdapter($this->logger, $adapters);
    }

    public function createConnection(DatabaseConfig $databaseConfig): void
    {
        $this->saveSslCertificate($databaseConfig);
        $this->connection = new MSSQLPdoConnection($this->logger, $databaseConfig);
    }

    public function testConnection(): void
    {
        $this->connection->testConnection();
    }

    /**
     * @param MssqlExportConfig $exportConfig
     */
    public function export(ExportConfig $exportConfig): array
    {
        if ($exportConfig->isCdcMode()) {
            $cdcName = $exportConfig->getTable()->getSchema() . '_' . $exportConfig->getTable()->getName();
            $this->getQueryFactory()->setFormat(MSSQLQueryFactory::ESCAPING_TYPE_PDO);
            $columns = $this->getQueryFactory()->getColumnsForSelect($exportConfig, $this->connection);

            $cdcExportConfig = clone $exportConfig;
            if (!empty($this->state['lastFetchedTime'])) {
                // @phpcs:disable Generic.Files.LineLength
                $query = <<<SQL
DECLARE @begin_time datetime, @end_time datetime, @from_lsn binary(10), @to_lsn binary(10);
SET @begin_time = CONVERT(DATETIME, '{$this->state['lastFetchedTime']}');
SET @end_time = GETDATE();
SET @from_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', @begin_time);
SET @from_lsn = ISNULL(sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', @begin_time), [sys].[fn_cdc_get_min_lsn]('$cdcName'));
SET @to_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time);
IF @to_lsn < @from_lsn
BEGIN
RAISERROR('The end LSN is less than the start LSN.', 16, 1);
END
SELECT $columns, IIF(__\$operation = 1, 1, 0) as is_deleted FROM cdc.fn_cdc_get_net_changes_$cdcName(@from_lsn, @to_lsn, 'all');
SQL;
                // @phpcs:enable Generic.Files.LineLength
                $cdcExportConfig->setQuery($query);
            }

            $sqlToLsnTime = <<<SQL
DECLARE @to_lsn binary(10);
SET @to_lsn = [sys].[fn_cdc_get_max_lsn]();
SELECT sys.fn_cdc_map_lsn_to_time(@to_lsn) as last_fetched_time;
SQL;
            $sqlToLsnTime = $this->connection->query($sqlToLsnTime);
            $lsnTimeResponse = $sqlToLsnTime->fetchAll();
            assert(count($lsnTimeResponse) === 1, 'Expected one row');
            $lsnTime = $lsnTimeResponse[0]['last_fetched_time'];

            try {
                $result = parent::export($cdcExportConfig);
            } catch (Throwable $e) {
                if (strpos($e->getMessage(), 'The end LSN is less than the start LSN') &&
                    $exportConfig->cdcModeFullLoadFallback()) {
                    $this->logger->info('CDC export failed, trying to export full table', [
                        'exception' => $e,
                    ]);
                    $result = parent::export($exportConfig);
                } else {
                    throw $e;
                }
            }
            $result['state']['lastFetchedTime'] = $lsnTime;
        } else {
            $result = parent::export($exportConfig);
        }

        return $result;
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        $result = $this->connection->query(sprintf(
            $this->getQueryFactory()->getIncrementalFetchingType() === MssqlDataType::INCREMENT_TYPE_BINARY ?
                'SELECT CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), MAX(%s)), 1) %s FROM %s.%s' :
                'SELECT MAX(%s) %s FROM %s.%s',
            $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->connection->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->connection->quoteIdentifier($exportConfig->getTable()->getName())
        ), $exportConfig->getMaxRetries())->fetchAll();

        return count($result) > 0 ? $result[0][$exportConfig->getIncrementalFetchingColumn()] : null;
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        try {
            $column = $this->createMetadataProvider()
               ->getTable($exportConfig->getTable())
               ->getColumns()
               ->getByName($exportConfig->getIncrementalFetchingColumn());
        } catch (ColumnNotFoundException $e) {
            throw new ColumnNotFoundException(sprintf(
                'Column "%s" specified for incremental fetching was not found.',
                $exportConfig->getIncrementalFetchingColumn(),
            ), 0, $e);
        }

        $this
            ->getQueryFactory()
            ->setIncrementalFetchingType(
                MssqlDataType::getIncrementalFetchingType($column->getName(), $column->getType())
            )
        ;
    }

    private function saveSslCertificate(DatabaseConfig $databaseConfig): void
    {
        if ($databaseConfig->hasSSLConnection() && $databaseConfig->getSslConnectionConfig()->hasCa()) {
            file_put_contents(
                '/usr/local/share/ca-certificates/mssql.crt',
                $databaseConfig->getSslConnectionConfig()->getCa()
            );
            Process::fromShellCommandline('update-ca-certificates')->mustRun();
        }
    }
}
