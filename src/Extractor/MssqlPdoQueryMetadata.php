<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\Exception\InvalidStateException;
use Keboola\DbExtractor\Adapter\PDO\PdoQueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;

/**
 * Query metadata for the PDO export path.
 *
 * The pdo_sqlsrv driver reports the literal string "string" in "native_type" for every SQL Server
 * type, so the default PdoQueryMetadata declares every column of a custom query as STRING. The
 * declared type name is in the driver specific "sqlsrv:decl_type" key instead. Reading it gives the
 * PDO path the same base types as the BCP path, which gets them from sp_describe_first_result_set.
 */
class MssqlPdoQueryMetadata extends PdoQueryMetadata
{
    public const DECLARED_TYPE_KEY = 'sqlsrv:decl_type';

    public function getColumns(): ColumnCollection
    {
        if ($this->columns === null) {
            $this->columns = $this->doGetDeclaredTypeColumns();
        }

        return $this->columns;
    }

    private function doGetDeclaredTypeColumns(): ColumnCollection
    {
        $columnsCount = $this->stmt->columnCount();
        $columns = [];
        for ($i = 0; $i < $columnsCount; $i++) {
            $columnMetadata = $this->stmt->getColumnMeta($i);
            if ($columnMetadata === false) {
                throw new InvalidStateException(sprintf('Cannot read metadata of PDO query column %d.', $i));
            }

            $builder = ColumnBuilder::create();
            $builder->setName($columnMetadata['name']);
            $builder->setType(self::resolveType($columnMetadata));
            $columns[] = $builder->build();
        }

        return new ColumnCollection($columns);
    }

    /**
     * @param array<mixed> $columnMetadata
     */
    private static function resolveType(array $columnMetadata): string
    {
        $declaredType = $columnMetadata[self::DECLARED_TYPE_KEY] ?? null;
        if (is_string($declaredType) && $declaredType !== '') {
            // The ODBC driver reports an identity column as e.g. "int identity".
            return (string) preg_replace('~\s+identity$~i', '', trim($declaredType));
        }

        // Another PDO driver, or a column the driver cannot describe (for example sql_variant).
        $nativeType = $columnMetadata['native_type'] ?? null;
        return is_string($nativeType) && $nativeType !== '' ? $nativeType : 'string';
    }
}
