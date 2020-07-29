<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata;

use Keboola\DbExtractor\Extractor\MetadataProvider;
use Keboola\DbExtractor\Extractor\PdoConnection;
use Keboola\DbExtractor\TableResultFormat\Exception\InvalidStateException;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

class MssqlMetadataProvider implements MetadataProvider
{
    private PdoConnection $pdo;

    /** @var TableCollection[] */
    private array $cache = [];

    public function __construct(PdoConnection $pdo)
    {
        $this->pdo = $pdo;
    }

    public function getTable(InputTable $table): Table
    {
        return $this
            ->listTables([$table])
            ->getByNameAndSchema($table->getName(), $table->getSchema());
    }

    /**
     * @param array|InputTable[] $whitelist
     * @param bool $loadColumns if false, columns metadata are NOT loaded, useful if there are a lot of tables
     */
    public function listTables(array $whitelist = [], bool $loadColumns = true): TableCollection
    {
        // Return cached value if present
        $cacheKey = md5(serialize(func_get_args()));
        if (isset($this->cache[$cacheKey])) {
            return $this->cache[$cacheKey];
        }

        /** @var TableBuilder[] $tableBuilders */
        $tableBuilders = [];

        /** @var ColumnBuilder[] $columnBuilders */
        $columnBuilders = [];

        $builder = MetadataBuilder::create();
        $tablesSql = MssqlSqlHelper::getTablesSql($whitelist, $this->pdo);
        $tables = $this->pdo->runQuery($tablesSql);
        foreach ($tables as $data) {
            $tableId = $data['TABLE_SCHEMA'] . '.' . $data['TABLE_NAME'];
            $tableBuilder = $this->processTable($data, $builder);
            $tableBuilders[$tableId] = $tableBuilder;

            if (!$loadColumns) {
                $tableBuilder->setColumnsNotExpected();
            }
        }

        if ($loadColumns) {
            $columnsSql = $whitelist ?
                MssqlSqlHelper::getColumnsSqlComplex($whitelist, $this->pdo) :
                MssqlSqlHelper::getColumnsSqlQuick();

            $columns = $this->pdo->runQuery($columnsSql);
            foreach ($columns as $data) {
                $tableId = $data['TABLE_SCHEMA'] . '.' . $data['TABLE_NAME'];
                $columnId = $data['COLUMN_NAME'] . '.' . $tableId;
                $tableBuilder = $tableBuilders[$tableId];

                // When "getColumnsSqlComplex" is used,
                // then one column can be present multiple times in result if has multiple constraints,
                // so column builder is reused
                if (isset($columnBuilders[$columnId])) {
                    $columnBuilder = $columnBuilders[$columnId];
                } else {
                    $columnBuilder = $tableBuilder->addColumn();
                    $columnBuilders[$columnId] = $columnBuilder;
                }

                $this->processColumn($data, $columnBuilder);
            }
        }

        return $builder->build();
    }

    private function processTable(array $data, MetadataBuilder $builder): TableBuilder
    {
        return $builder
            ->addTable()
            ->setName($data['TABLE_NAME'])
            ->setCatalog($data['TABLE_CATALOG'])
            ->setSchema($data['TABLE_SCHEMA'])
            ->setType($data['TABLE_TYPE']);
    }

    private function processColumn(array $data, ColumnBuilder $columnBuilder): ColumnBuilder
    {
        $columnBuilder = $columnBuilder
            ->setName($data['COLUMN_NAME'], false)
            ->setOrdinalPosition((int) $data['ORDINAL_POSITION']);

        // Type and length
        if (isset($data['DATA_TYPE'])) {
            $columnBuilder
                ->setType($data['DATA_TYPE'])
                ->setLength(MssqlSqlHelper::getFieldLength($data));
        } else {
            $columnBuilder->setType('USER_DEFINED_TYPE');
        }

        // Nullable
        if (isset($data['IS_NULLABLE'])) {
            $columnBuilder->setNullable($data['IS_NULLABLE'] === 'YES' || $data['IS_NULLABLE'] === '1');
        }

        // Default
        if (isset($data['COLUMN_DEFAULT'])) {
            $columnBuilder->setDefault($data['COLUMN_DEFAULT']);
        }

        // Primary key
        if (isset($data['pk_name'])) {
            $columnBuilder->setPrimaryKey(true);
        }

        // Unique key
        if (isset($data['uk_name'])) {
            $columnBuilder->setUniqueKey(true);
        }

        // Foreign key
        if (isset($data['fk_name'])) {
            try {
                $columnBuilder
                    ->addForeignKey()
                    ->setName($data['fk_name'])
                    ->setRefSchema($data['REFERENCED_SCHEMA_NAME'])
                    ->setRefTable($data['REFERENCED_TABLE_NAME'])
                    ->setRefColumn($data['REFERENCED_COLUMN_NAME']);
            } catch (InvalidStateException $e) {
                // FK is already set
            }
        }

        // Auto increment
        if (isset($data['is_identity'])) {
            $columnBuilder->setAutoIncrement(true);
        }

        return $columnBuilder;
    }
}
