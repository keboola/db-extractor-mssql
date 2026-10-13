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
 * type. The default PdoQueryMetadata reads that key, so a custom query exported through the PDO
 * adapter declares every column as STRING in the manifest. The declared type name is available in
 * the driver specific "sqlsrv:decl_type" key instead.
 *
 * getDeclaredTypeColumns() reads that key, so the PDO path can report the same base types as the
 * BCP path, which gets real type names from sp_describe_first_result_set.
 *
 * getColumns() is deliberately NOT changed. It stays the behaviour of every existing configuration.
 * The declared types are opt-in through the "queryNativeTypes" row option and are read only by
 * MssqlManifestGenerator.
 */
class MssqlPdoQueryMetadata extends PdoQueryMetadata
{
    public const DECLARED_TYPE_KEY = 'sqlsrv:decl_type';

    private ?ColumnCollection $declaredTypeColumns = null;

    public function getDeclaredTypeColumns(): ColumnCollection
    {
        if ($this->declaredTypeColumns === null) {
            $this->declaredTypeColumns = $this->doGetDeclaredTypeColumns();
        }

        return $this->declaredTypeColumns;
    }

    private function doGetDeclaredTypeColumns(): ColumnCollection
    {
        $columnsCount = $this->stmt->columnCount();
        $columns = [];
        for ($i = 0; $i < $columnsCount; $i++) {
            $columnMetadata = $this->stmt->getColumnMeta($i);
            $name = is_array($columnMetadata) ? ($columnMetadata['name'] ?? null) : null;
            if (!is_string($name)) {
                throw new InvalidStateException('Missing key "name" in PDO query column\'s metadata.');
            }

            $builder = ColumnBuilder::create();
            $builder->setName($name);
            $builder->setType(self::resolveType(is_array($columnMetadata) ? $columnMetadata : []));
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
            return $declaredType;
        }

        // Another PDO driver, or a column the driver cannot describe (for example sql_variant).
        // Keep the default behaviour, which is what this column got before this class existed.
        $nativeType = $columnMetadata['native_type'] ?? null;
        return is_string($nativeType) && $nativeType !== '' ? $nativeType : 'string';
    }
}
