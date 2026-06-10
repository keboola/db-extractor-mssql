<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvWriter;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class MSSQLResultWriter extends DefaultResultWriter
{
    private ?ExportConfig $currentExportConfig = null;

    protected function hasCsvHeader(ExportConfig $exportConfig): bool
    {
        return false;
    }

    public function writeToCsv(
        QueryResult $result,
        ExportConfig $exportConfig,
        string $csvFilePath,
    ): ExportResult {
        // Stash the config so createCsvWriter() can decide which writer to use (see below).
        $this->currentExportConfig = $exportConfig;
        return parent::writeToCsv($result, $exportConfig, $csvFilePath);
    }

    protected function createCsvWriter(string $csvFilePath): CsvWriter
    {
        // Only CDC exports need NULL serialized as an unquoted empty field (SUPPORT-16443).
        // CDC mode forces the PDO adapter, whose default CsvWriter writes NULL as "", which a typed
        // Snowflake DATE/TIMESTAMP column rejects ("Timestamp '' is not recognized"). Non-CDC PDO
        // exports (disableBcp, BCP→PDO fallback) keep the default writer, preserving their
        // long-standing NULL-as-"" behaviour so existing configurations are unaffected.
        if ($this->currentExportConfig instanceof MssqlExportConfig
            && $this->currentExportConfig->isCdcMode()
        ) {
            $dir = dirname($csvFilePath);
            if (!is_dir($dir)) {
                mkdir($dir, 0777, true);
            }
            return new NullAwareCsvWriter($csvFilePath);
        }

        return parent::createCsvWriter($csvFilePath);
    }
}
