<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvWriter;
use Keboola\Csv\Exception;

/**
 * CSV writer that produces unquoted empty fields for NULL values.
 *
 * Snowflake's EMPTY_FIELD_AS_NULL (default TRUE) reliably converts unquoted empty fields
 * to SQL NULL during COPY INTO, regardless of NULL_IF configuration. This ensures correct
 * NULL handling when importing into typed tables with date/timestamp columns.
 *
 * Implementation note: rowToStr() re-implements the parent's quote/escape logic because
 * CsvWriter::rowToStr() coerces null via ($column ?? '') before enclosing, giving us no
 * hook to emit an unquoted field. The non-NULL branch mirrors CsvWriter ^3.4 internals
 * (getEnclosure(), getDelimiter()); if those change, this override must be updated.
 * The line break is hardcoded to "\n" — parent's $lineBreak is private and inaccessible.
 */
class NullAwareCsvWriter extends CsvWriter
{
    public function rowToStr(array $row): string
    {
        $return = [];
        foreach ($row as $column) {
            if ($column === null) {
                $return[] = '';
            } else {
                if (!(
                    is_scalar($column)
                    || (
                        is_object($column)
                        && method_exists($column, '__toString')
                    )
                )) {
                    throw new Exception(
                        'Cannot write data into column: ' . var_export($column, true),
                        Exception::WRITE_ERROR,
                    );
                }
                $return[] = $this->getEnclosure() .
                    str_replace($this->getEnclosure(), str_repeat($this->getEnclosure(), 2), (string) $column) .
                    $this->getEnclosure();
            }
        }
        // Hardcoded "\n": parent's $lineBreak is private; safe because this writer is only
        // constructed with default options (which use "\n").
        return implode($this->getDelimiter(), $return) . "\n";
    }
}
