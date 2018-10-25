<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\GenericStorage;

class MssqlDataType extends GenericStorage
{
    const DATE_TYPES = ["date"];
    const TIMESTAMP_TYPES = [
        "datetime", "datetime2", "smalldatetime", "datetimeoffset"
    ];
    const FLOATING_POINT_TYPES = [
        "real", "float"
    ];

    const BOOLEAN_TYPES = ["bit"];

    const INTEGER_TYPES = [
        "integer", "int", "smallint", "tinyint", "bigint"
    ];

    const FIXED_NUMERIC_TYPES = [
        "numeric", "decimal", "money", "smallmoney"
    ];
}
