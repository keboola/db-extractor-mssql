<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\GenericStorage;

class MssqlDataType extends GenericStorage
{
    public const DATE_TYPES = ["date"];

    public const TIMESTAMP_TYPES = [
        "datetime", "datetime2", "smalldatetime", "datetimeoffset",
    ];

    public const FLOATING_POINT_TYPES = [
        "real", "float",
    ];

    public const BOOLEAN_TYPES = ["bit"];

    public const INTEGER_TYPES = [
        "integer", "int", "smallint", "tinyint", "bigint",
    ];

    public const FIXED_NUMERIC_TYPES = [
        "numeric", "decimal", "money", "smallmoney",
    ];
}
