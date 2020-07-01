<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Extractor\Adapters\PdoAdapter;

class QueryFactory
{
    public const ESCAPING_TYPE_BCP = 'BCP';
    public const ESCAPING_TYPE_PDO = 'PDO';

    private PdoAdapter $pdoAdapter;

    private MetadataProvider $metadataProvider;

    private array $state;

    public function __construct(
        PdoAdapter $pdoAdapter,
        MetadataProvider $metadataProvider,
        array $state
    ) {
        $this->pdoAdapter = $pdoAdapter;
        $this->metadataProvider = $metadataProvider;
        $this->state = $state;
    }

    public function create(array $table, ?array $incrementalFetching, string $format): string
    {
        $isAdvancedQuery = array_key_exists('query', $table);
        if ($isAdvancedQuery) {
            return $table['query'];
        }

        $sql = [];
        $sql[] = 'SELECT';

        if (isset($incrementalFetching['limit'])) {
            $sql[] = sprintf('TOP %d', $incrementalFetching['limit']);
        }

        $columns = $this->metadataProvider->getColumnsMetadata($table);
        $sql[] = sprintf(
            '%s FROM %s.%s',
            $columns ? $this->getColumnsForSelect($columns, $format) : '*',
            $this->pdoAdapter->quoteIdentifier($table['table']['schema']),
            $this->pdoAdapter->quoteIdentifier($table['table']['tableName'])
        );

        if ($table['nolock'] ?? false) {
            $sql[] = 'WITH(NOLOCK)';
        }

        if ($incrementalFetching) {
            if (isset($this->state['lastFetchedRow'])) {
                $sql[] = sprintf(
                    'WHERE %s >= %s',
                    $this->pdoAdapter->quoteIdentifier($incrementalFetching['column']),
                    $this->shouldQuoteComparison($incrementalFetching['type'])
                        ? $this->pdoAdapter->quote($this->state['lastFetchedRow'])
                        : $this->state['lastFetchedRow']
                );
            }

            if ($this->hasIncrementalLimit($incrementalFetching)) {
                $sql[] = sprintf(
                    'ORDER BY %s',
                    $this->pdoAdapter->quoteIdentifier($incrementalFetching['column'])
                );
            }
        }

        return implode(' ', $sql);
    }

    public function columnToBcpSql(array $column): string
    {
        // BCP exports CSV data without surrounding double quotes,
        // ... so double quotes are added in SQL

        $datatype = new MssqlDataType(
            $column['type'],
            array_intersect_key($column, array_flip(MssqlDataType::DATATYPE_KEYS))
        );
        $colstr = $escapedColumnName = $this->pdoAdapter->quoteIdentifier($column['name']);
        if ($datatype->getType() === 'timestamp') {
            $colstr = sprintf('CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), %s), 1)', $colstr);
        } else if ($datatype->getBasetype() === 'STRING') {
            if ($datatype->getType() === 'text'
                || $datatype->getType() === 'ntext'
                || $datatype->getType() === 'xml'
            ) {
                $colstr = sprintf('CAST(%s as nvarchar(max))', $colstr);
            }
            $colstr = sprintf('REPLACE(%s, char(34), char(34) + char(34))', $colstr);
            if ($datatype->isNullable()) {
                $colstr = sprintf("COALESCE(%s,'')", $colstr);
            }
            $colstr = sprintf('char(34) + %s + char(34)', $colstr);
        } else if ($datatype->getBasetype() === 'TIMESTAMP'
            && strtoupper($datatype->getType()) !== 'SMALLDATETIME'
        ) {
            $colstr = sprintf('CONVERT(DATETIME2(0),%s)', $colstr);
        }
        if ($colstr !== $escapedColumnName) {
            return $colstr . ' AS ' . $escapedColumnName;
        }
        return $colstr;
    }

    public function columnToPdoSql(array $column): string
    {
        $datatype = new MssqlDataType(
            $column['type'],
            array_intersect_key($column, array_flip(MssqlDataType::DATATYPE_KEYS))
        );
        $colstr = $escapedColumnName = $this->pdoAdapter->quoteIdentifier($column['name']);
        if ($datatype->getType() === 'timestamp') {
            $colstr = sprintf('CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), %s), 1)', $colstr);
        } else {
            if ($datatype->getType() === 'text'
                || $datatype->getType() === 'ntext'
                || $datatype->getType() === 'xml'
            ) {
                $colstr = sprintf('CAST(%s as nvarchar(max))', $colstr);
            }
        }
        if ($colstr !== $escapedColumnName) {
            return $colstr . ' AS ' . $escapedColumnName;
        }
        return $colstr;
    }

    private function getColumnsForSelect(array $columns, string $format): string
    {
        if ($format === self::ESCAPING_TYPE_BCP) {
            return implode(', ', array_map(fn (array $column) => $this->columnToBcpSql($column), $columns));
        } else if ($format === self::ESCAPING_TYPE_PDO) {
            return implode(', ', array_map(fn (array $column) => $this->columnToPdoSql($column), $columns));
        }

        throw new \LogicException(sprintf('Unexpected format: "%s"', $format));
    }

    private function shouldQuoteComparison(string $type): bool
    {
        if ($type === MssqlDataType::INCREMENT_TYPE_NUMERIC || $type === MssqlDataType::INCREMENT_TYPE_BINARY) {
            return false;
        }
        return true;
    }

    protected function hasIncrementalLimit(?array $incrementalFetching): bool
    {
        if (!$incrementalFetching) {
            return false;
        }
        if (isset($incrementalFetching['limit']) && (int) $incrementalFetching['limit'] > 0) {
            return true;
        }
        return false;
    }
}
