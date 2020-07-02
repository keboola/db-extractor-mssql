<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use LogicException;
use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Column;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class QueryFactory
{
    public const ESCAPING_TYPE_BCP = 'BCP';
    public const ESCAPING_TYPE_PDO = 'PDO';

    private PdoConnection $pdo;

    private MetadataProvider $metadataProvider;

    private array $state;

    public function __construct(
        PdoConnection $pdo,
        MetadataProvider $metadataProvider,
        array $state
    ) {
        $this->pdo = $pdo;
        $this->metadataProvider = $metadataProvider;
        $this->state = $state;
    }

    public function create(MssqlExportConfig $exportConfig, string $format, ?string $incrementalFetchingType): string
    {
        if ($exportConfig->hasQuery()) {
            return $exportConfig->getQuery();
        }

        $sql = [];
        $sql[] = 'SELECT';

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $sql[] = sprintf('TOP %d', $exportConfig->getIncrementalFetchingLimit());
        }

        $sql[] = sprintf(
            '%s FROM %s.%s',
            $this->getColumnsForSelect($exportConfig, $format),
            $this->pdo->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->pdo->quoteIdentifier($exportConfig->getTable()->getName())
        );

        if ($exportConfig->getNoLock()) {
            $sql[] = 'WITH(NOLOCK)';
        }

        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            if (isset($this->state['lastFetchedRow'])) {
                $sql[] = sprintf(
                    'WHERE %s >= %s',
                    $this->pdo->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                    $this->shouldQuoteComparison($incrementalFetchingType)
                        ? $this->pdo->quote($this->state['lastFetchedRow'])
                        : $this->state['lastFetchedRow']
                );
            }
        }

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $sql[] = sprintf(
                'ORDER BY %s',
                $this->pdo->quoteIdentifier($exportConfig->getIncrementalFetchingColumn())
            );
        }

        return implode(' ', $sql);
    }

    public function columnToBcpSql(Column $column): string
    {
        // BCP exports CSV data without surrounding double quotes,
        // ... so double quotes are added in SQL

        $datatype = $this->getColumnDatatype($column);
        $escapedColumnName = $this->pdo->quoteIdentifier($column->getName());
        $colStr = $escapedColumnName;

        if ($datatype->getType() === 'timestamp') {
            $colStr = sprintf('CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), %s), 1)', $colStr);
        } else if ($datatype->getBasetype() === 'STRING') {
            if ($datatype->getType() === 'text'
                || $datatype->getType() === 'ntext'
                || $datatype->getType() === 'xml'
            ) {
                $colStr = sprintf('CAST(%s as nvarchar(max))', $colStr);
            }
            $colStr = sprintf('REPLACE(%s, char(34), char(34) + char(34))', $colStr);
            if ($datatype->isNullable()) {
                $colStr = sprintf("COALESCE(%s,'')", $colStr);
            }
            $colStr = sprintf('char(34) + %s + char(34)', $colStr);
        } else if ($datatype->getBasetype() === 'TIMESTAMP'
            && strtoupper($datatype->getType()) !== 'SMALLDATETIME'
        ) {
            $colStr = sprintf('CONVERT(DATETIME2(0),%s)', $colStr);
        }
        if ($colStr !== $escapedColumnName) {
            return $colStr . ' AS ' . $escapedColumnName;
        }
        return $colStr;
    }

    public function columnToPdoSql(Column $column): string
    {
        $datatype = $this->getColumnDatatype($column);
        $escapedColumnName = $this->pdo->quoteIdentifier($column->getName());
        $colStr = $escapedColumnName;

        if ($datatype->getType() === 'timestamp') {
            $colStr = sprintf('CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), %s), 1)', $colStr);
        } else {
            if ($datatype->getType() === 'text'
                || $datatype->getType() === 'ntext'
                || $datatype->getType() === 'xml'
            ) {
                $colStr = sprintf('CAST(%s as nvarchar(max))', $colStr);
            }
        }
        if ($colStr !== $escapedColumnName) {
            return $colStr . ' AS ' . $escapedColumnName;
        }
        return $colStr;
    }

    private function getColumnDatatype(Column $column): MssqlDataType
    {
        return new MssqlDataType(
            $column->getType(),
            [
                'type' => $column->getType(),
                'length' => $column->hasLength() ? $column->getLength() : null,
                'nullable' => $column->hasNullable() ? $column->isNullable() : null,
                'default' => $column->hasDefault() ? (string) $column->getDefault() : null,
            ]
        );
    }

    private function getColumnsForSelect(ExportConfig $exportConfig, string $format): string
    {
        $columns = $this->metadataProvider->getTable($exportConfig->getTable())->getColumns();
        // Map column names (from config or all) to metadata objects, and then format them for SELECT.
        $columnNames = $exportConfig->hasColumns() ? $exportConfig->getColumns() : $columns->getNames();
        if ($format === self::ESCAPING_TYPE_BCP) {
            return implode(', ', array_map(
                fn (string $name) => $this->columnToBcpSql($columns->getByName($name)),
                $columnNames
            ));
        } else if ($format === self::ESCAPING_TYPE_PDO) {
            return implode(', ', array_map(
                fn (string $name) => $this->columnToPdoSql($columns->getByName($name)),
                $columnNames
            ));
        }

        throw new LogicException(sprintf('Unexpected format: "%s"', $format));
    }

    private function shouldQuoteComparison(?string $type): bool
    {
        if ($type === null) {
            throw new \InvalidArgumentException(
                'Incremental fetching type should be set if calling "shouldQuoteComparison".'
            );
        }

        if ($type === MssqlDataType::INCREMENT_TYPE_NUMERIC || $type === MssqlDataType::INCREMENT_TYPE_BINARY) {
            return false;
        }
        return true;
    }
}
