<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use DateTimeImmutable;
use InvalidArgumentException;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Query\QueryFactory;
use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Metadata\MssqlMetadataProvider;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Column;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractorConfig\Incremental\WindowBoundResolver;
use LogicException;

class MSSQLQueryFactory implements QueryFactory
{
    public const ESCAPING_TYPE_BCP = 'BCP';
    public const ESCAPING_TYPE_PDO = 'PDO';

    protected string $format;

    protected array $state;

    protected MssqlMetadataProvider $metadataProvider;

    protected string $incrementalFetchingType;

    private WindowBoundResolver $resolver;

    private DateTimeImmutable $now;

    public function __construct(
        array $state,
        MssqlMetadataProvider $metadataProvider,
        ?WindowBoundResolver $resolver = null,
        ?DateTimeImmutable $now = null,
    ) {
        $this->state = $state;
        $this->metadataProvider = $metadataProvider;
        $this->resolver = $resolver ?? new WindowBoundResolver();
        $this->now = $now ?? new DateTimeImmutable('now');
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

        $where = $this->createWhere($exportConfig, $connection);
        if ($where !== null) {
            $sql[] = $where;
        }

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $sql[] = sprintf(
                'ORDER BY %s',
                $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            );
        }

        return implode(' ', $sql);
    }

    /**
     * Builds the incremental-fetching WHERE clause. Mirrors db-extractor-adapter's DefaultQueryFactory::
     * createWhere() (mssql reimplements the query factory, so the mode branch is replicated here with
     * mssql's own literal formatting):
     *  - window mode  => strict [start, end] range resolved via WindowBoundResolver; the stored watermark
     *                    is intentionally ignored (the range is re-scanned every run, deduplicated by PK).
     *  - watermark mode (default) => resume from the stored watermark (col >= lastFetchedRow), optionally
     *                    lowered by a lookback margin so a late-committing row (assigned below the
     *                    watermark, visible only afterwards) is re-scanned. No watermark yet (first run)
     *                    => no predicate, i.e. a full fetch, exactly as before.
     */
    private function createWhere(MssqlExportConfig $exportConfig, DbConnection $connection): ?string
    {
        if (!$exportConfig->isIncrementalFetching()) {
            return null;
        }

        if ($exportConfig->isIncrementalFetchingWindowMode()) {
            return $this->createWindowWhere($exportConfig, $connection);
        }

        if (!isset($this->state['lastFetchedRow'])) {
            return null;
        }

        $lowerBound = (string) $this->state['lastFetchedRow'];
        if ($exportConfig->hasIncrementalFetchingLookback()) {
            $this->guardBoundsSupported();
            $lowerBound = $this->resolver->resolveLookbackLowerBound(
                $lowerBound,
                (string) $exportConfig->getIncrementalFetchingLookback(),
                $exportConfig->getIncrementalColumnType(),
            );
        }

        // intentionally ">=": the last row is re-included; the storage deduplication process handles it.
        return sprintf(
            'WHERE %s >= %s',
            $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->formatIncrementalValue($lowerBound, $connection),
        );
    }

    private function createWindowWhere(MssqlExportConfig $exportConfig, DbConnection $connection): ?string
    {
        // No bounds configured in window mode => no predicate (the watermark is intentionally ignored).
        if (!$exportConfig->hasIncrementalFetchingWindow()) {
            return null;
        }

        $this->guardBoundsSupported();

        $columnType = $exportConfig->getIncrementalColumnType();
        $lower = $this->resolver->resolveLowerBound(
            $exportConfig->getIncrementalFetchingWindowStart(),
            $columnType,
            $this->now,
        );
        $upper = $this->resolver->resolveUpperBound(
            $exportConfig->getIncrementalFetchingWindowEnd(),
            $columnType,
            $this->now,
        );

        $column = $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn());
        $conditions = [];
        if ($lower !== null) {
            $conditions[] = sprintf('%s >= %s', $column, $this->formatIncrementalValue($lower, $connection));
        }
        if ($upper !== null) {
            $conditions[] = sprintf('%s <= %s', $column, $this->formatIncrementalValue($upper, $connection));
        }

        return $conditions === [] ? null : 'WHERE ' . implode(' AND ', $conditions);
    }

    /**
     * A binary/rowversion column has no ordered value that a window range or a lookback offset could be
     * computed against. In practice db-extractor-common's BaseExtractor::export() already rejects this
     * before the query is built (getIncrementalFetchingColumnType() returns null); this is a defensive
     * second guard so the factory stays consistent and clear even when driven directly. Plain watermark
     * fetching on a binary column stays supported — only window/lookback bounds are rejected here.
     */
    private function guardBoundsSupported(): void
    {
        if ($this->incrementalFetchingType === MssqlDataType::INCREMENT_TYPE_BINARY) {
            throw new UserException(
                'Incremental fetching window/lookback is not supported for a binary/rowversion column.',
            );
        }
    }

    /**
     * Formats an incremental-fetching literal for the WHERE clause according to the column's internal
     * type — the same mapping the plain-watermark path has always used: numeric and binary tokens are
     * inlined raw, a "quotable" (smalldatetime) value is single-quoted, and a datetime value is wrapped
     * in CONVERT(DATETIME2, ..., 120) so the ODBC-canonical string compares correctly.
     */
    private function formatIncrementalValue(string $value, DbConnection $connection): string
    {
        return match ($this->incrementalFetchingType) {
            MssqlDataType::INCREMENT_TYPE_NUMERIC,
            MssqlDataType::INCREMENT_TYPE_BINARY => $value,
            MssqlDataType::INCREMENT_TYPE_QUOTABLE => $connection->quote($value),
            MssqlDataType::INCREMENT_TYPE_DATETIME => sprintf(
                'CONVERT(DATETIME2, %s, 120)',
                $connection->quote($value),
            ),
            default => throw new InvalidArgumentException(
                sprintf('Unknown incremental fetching type "%s"', $this->incrementalFetchingType),
            ),
        };
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
