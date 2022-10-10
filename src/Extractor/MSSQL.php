<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\FallbackExportAdapter;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
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
