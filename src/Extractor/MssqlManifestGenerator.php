<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractor\Manifest\DefaultManifestGenerator;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

/**
 * Lets a custom query exported through the PDO adapter report real base types.
 *
 * This is opt-in through the "queryNativeTypes" row option, because an output table that an earlier
 * run created from the PDO path has STRING columns. A row that turns the option on must get a new
 * output table.
 *
 * The BCP path is not touched: BcpQueryMetadata already reports real type names.
 */
class MssqlManifestGenerator extends DefaultManifestGenerator
{
    public function generate(ExportConfig $exportConfig, ExportResult $exportResult, bool $legacy = false): array
    {
        $queryMetadata = $exportResult->getQueryMetadata();

        if ($exportConfig instanceof MssqlExportConfig
            && $exportConfig->hasQuery()
            && $exportConfig->hasQueryNativeTypes()
            && $queryMetadata instanceof MssqlPdoQueryMetadata
        ) {
            $exportResult = new ExportResult(
                $exportResult->getCsvPath(),
                $exportResult->getRowsCount(),
                new ColumnCollectionQueryMetadata($queryMetadata->getDeclaredTypeColumns()),
                $exportResult->hasCsvHeader(),
                $exportResult->getIncFetchingColMaxValue(),
            );
        }

        return parent::generate($exportConfig, $exportResult, $legacy);
    }
}
