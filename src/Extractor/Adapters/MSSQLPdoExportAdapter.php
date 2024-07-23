<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor\Adapters;

use Keboola\DbExtractor\Adapter\PDO\PdoExportAdapter;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\PdoAdapterSkippedException;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use Keboola\DbExtractor\Extractor\MSSQLQueryFactory;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;

class MSSQLPdoExportAdapter extends PdoExportAdapter
{
    /** @var MSSQLQueryFactory $simpleQueryFactory */
    protected QueryFactory $simpleQueryFactory;

    public function __construct(
        LoggerInterface $logger,
        MSSQLPdoConnection $connection,
        MSSQLQueryFactory $queryFactory,
        DefaultResultWriter $resultWriter,
        string $dataDir,
        array $state,
    ) {
        parent::__construct($logger, $connection, $queryFactory, $resultWriter, $dataDir, $state);
    }

    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        if (!$exportConfig instanceof MssqlExportConfig) {
            throw new InvalidArgumentException('MssqlExportConfig expected.');
        }

        if ($exportConfig->isFallbackDisabled()) {
            throw new PdoAdapterSkippedException('Disabled in configuration.');
        }

        return parent::export($exportConfig, $csvFilePath);
    }

    protected function createSimpleQuery(ExportConfig $exportConfig): string
    {
        return $this
            ->simpleQueryFactory
            ->setFormat(MSSQLQueryFactory::ESCAPING_TYPE_PDO)
            ->create($exportConfig, $this->connection)
        ;
    }
}
