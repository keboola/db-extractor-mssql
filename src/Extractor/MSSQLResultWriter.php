<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class MSSQLResultWriter extends DefaultResultWriter
{
    protected function hasCsvHeader(ExportConfig $exportConfig): bool
    {
        return false;
    }
}
