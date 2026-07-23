<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata;

/**
 * Parsed MSSQL "system_type_name" value returned by "sp_describe_first_result_set",
 * e.g. "datetime2(7)", "decimal(18,2)", "nvarchar(max)".
 *
 * The bare type name and the length are separated, so the type name can be matched
 * by exact-match basetype mapping (GenericStorage::getBasetype).
 *
 * The type name is always returned lowercase.
 */
final class SystemTypeName
{
    private string $type;

    private ?string $length;

    public static function parse(string $systemTypeName): self
    {
        if (preg_match('~^(?<type>[^(]+)\((?<qualifier>[^)]+)\)$~', trim($systemTypeName), $matches) !== 1) {
            return new self(strtolower(trim($systemTypeName)), null);
        }

        $type = strtolower(trim($matches['type']));
        return new self($type, self::normalizeLength($type, trim($matches['qualifier'])));
    }

    private static function normalizeLength(string $type, string $qualifier): ?string
    {
        if (strtolower($qualifier) === 'max') {
            return null;
        }

        // Consistent with the table export path, see MssqlSqlHelper::getFieldLength()
        if (in_array($type, MssqlSqlHelper::DATE_TIME_TYPES, true)) {
            return null;
        }

        return str_replace(' ', '', $qualifier);
    }

    private function __construct(string $type, ?string $length)
    {
        $this->type = $type;
        $this->length = $length;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function getLength(): ?string
    {
        return $this->length;
    }
}
