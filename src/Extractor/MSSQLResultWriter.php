<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvWriter;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class MSSQLResultWriter extends DefaultResultWriter
{
    protected function hasCsvHeader(ExportConfig $exportConfig): bool
    {
        return false;
    }

    protected function createCsvWriter(string $csvFilePath): CsvWriter
    {
        $dir = dirname($csvFilePath);
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }
        return new NullAwareCsvWriter($csvFilePath);
    }
}
