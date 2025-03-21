<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use InvalidArgumentException;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;
use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Metadata\MssqlMetadataProvider;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Column;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use LogicException;

class MSSQLQueryFactory implements QueryFactory
{
    public const ESCAPING_TYPE_BCP = 'BCP';
    public const ESCAPING_TYPE_PDO = 'PDO';

    protected string $format;

    protected array $state;

    protected MssqlMetadataProvider $metadataProvider;

    protected string $incrementalFetchingType;

    public function __construct(array $state, MssqlMetadataProvider $metadataProvider)
    {
        $this->state = $state;
        $this->metadataProvider = $metadataProvider;
    }

    public function setFormat(string $format): self
    {
        $this->format = $format;
        return $this;
    }

    public function setIncrementalFetchingType(string $incrementalFetchingType): self
    {
        $this->incrementalFetchingType = $incrementalFetchingType;
        return $this;
    }

    public function getIncrementalFetchingType(): string
    {
        return $this->incrementalFetchingType;
    }


    public function create(ExportConfig $exportConfig, DbConnection $connection): string
    {
        if (!($exportConfig instanceof MssqlExportConfig)) {
            throw new ApplicationException();
        }

        $sql = [];
        $sql[] = 'SELECT';

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $sql[] = sprintf('TOP %d', $exportConfig->getIncrementalFetchingLimit());
        }

        $sql[] = sprintf(
            '%s FROM %s.%s',
            $this->getColumnsForSelect($exportConfig, $connection),
            $connection->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $connection->quoteIdentifier($exportConfig->getTable()->getName()),
        );

        if ($exportConfig->getNoLock()) {
            $sql[] = 'WITH(NOLOCK)';
        }

        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            $sql[] = sprintf(
                'WHERE %s >= %s',
                $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                match ($this->incrementalFetchingType) {
                    MssqlDataType::INCREMENT_TYPE_NUMERIC,
                    MssqlDataType::INCREMENT_TYPE_BINARY => $this->state['lastFetchedRow'],
                    MssqlDataType::INCREMENT_TYPE_QUOTABLE => $connection->quote($this->state['lastFetchedRow']),
                    MssqlDataType::INCREMENT_TYPE_DATETIME => sprintf(
                        'CONVERT(DATETIME2, %s, 120)',
                        $connection->quote($this->state['lastFetchedRow']),
                    ),
                    default => throw new InvalidArgumentException(
                        sprintf('Unknown incremental fetching type "%s"', $this->incrementalFetchingType),
                    ),
                },
            );
        }

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $sql[] = sprintf(
                'ORDER BY %s',
                $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            );
        }

        return implode(' ', $sql);
    }


    public function columnToBcpSql(Column $column, DbConnection $connection): string
    {
        // BCP exports CSV data without surrounding double quotes,
        // ... so double quotes are added in SQL

        $datatype = $this->getColumnDatatype($column);
        $escapedColumnName = $connection->quoteIdentifier($column->getName());
        $colStr = $escapedColumnName;

        if ($datatype->getType() === 'timestamp') {
            $colStr = sprintf('CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), %s), 1)', $colStr);
        } elseif ($datatype->getBasetype() === 'STRING') {
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
        } elseif ($datatype->getBasetype() === 'TIMESTAMP'
            && strtoupper($datatype->getType()) !== 'SMALLDATETIME'
        ) {
            $colStr = sprintf('CONVERT(DATETIME2(3),%s)', $colStr);
        }
        if ($colStr !== $escapedColumnName) {
            return $colStr . ' AS ' . $escapedColumnName;
        }
        return $colStr;
    }

    public function columnToPdoSql(Column $column, DbConnection $connection): string
    {
        $datatype = $this->getColumnDatatype($column);
        $escapedColumnName = $connection->quoteIdentifier($column->getName());
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
        $options = [];
        if ($column->hasLength()) {
            $options['length'] = $column->getLength();
        }
        if ($column->hasNullable()) {
            $options['nullable'] = $column->isNullable();
        }
        if ($column->hasDefault()) {
            $options['default'] = (string) $column->getDefault();
        }
        return new MssqlDataType($column->getType(), $options);
    }

    public function getColumnsForSelect(ExportConfig $exportConfig, DbConnection $connection): string
    {
        $columns = $this->metadataProvider->getTable($exportConfig->getTable())->getColumns();
        // Map column names (from config or all) to metadata objects, and then format them for SELECT.
        $columnNames = $exportConfig->hasColumns() ? $exportConfig->getColumns() : $columns->getNames();
        if ($this->format === self::ESCAPING_TYPE_BCP) {
            return implode(', ', array_map(
                fn (string $name) => $this->columnToBcpSql($columns->getByName($name), $connection),
                $columnNames,
            ));
        } elseif ($this->format === self::ESCAPING_TYPE_PDO) {
            return implode(', ', array_map(
                fn (string $name) => $this->columnToPdoSql($columns->getByName($name), $connection),
                $columnNames,
            ));
        }

        throw new LogicException(sprintf('Unexpected format: "%s"', $this->format));
    }
}
