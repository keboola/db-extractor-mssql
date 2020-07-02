<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata;

use Keboola\Datatype\Definition\Common;
use Keboola\DbExtractor\Extractor\MssqlDataType;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\DefaultManifestSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Column;

class MssqlManifestSerializer extends DefaultManifestSerializer
{
    protected function columnToDatatype(Column $column, array $options): Common
    {
        return new MssqlDataType($column->getType(), $options);
    }
}
