<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata;

use Keboola\Datatype\Definition\Common;
use Keboola\DbExtractor\Extractor\MssqlDataType;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\DefaultManifestSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Column;

class MssqlManifestSerializer extends DefaultManifestSerializer
{
    protected function columnToDatatype(Column $column, array $options): Common
    {
        return new MssqlDataType($column->getType(), $options);
    }

    public function serializeColumn(Column $column): array
    {
        // Datatype metadata
        $options = [
            'type' => $column->getType(),
            'length' => $column->hasLength() ? $column->getLength() : null,
            'nullable' => $column->hasNullable() ? $column->isNullable() : null,
            'default' => $column->hasDefault() ? (string) $column->getDefault() : null,
        ];
        $options = array_filter($options, fn($value) => $value !== null); // remove null values
        $datatype = $this->columnToDatatype($column, $options);
        $columnMetadata = $datatype->toMetadata();

        // Non-datatype metadata
        $nonDatatypeMetadata = [
            'sourceName' => $column->getName(),
            'sanitizedName' => $column->getSanitizedName(),
            'ordinalPosition' => $column->hasOrdinalPosition() ? $column->getOrdinalPosition() : null,
            'autoIncrement' => $column->isAutoIncrement() ?: null,
            'autoIncrementValue' => $column->hasAutoIncrementValue() ? $column->getAutoIncrementValue() : null,
            'description' => $column->hasDescription() ? $column->getDescription() : null,
        ];

        if ($column->isPrimaryKey()) {
            $nonDatatypeMetadata['primaryKey'] = $column->isPrimaryKey();
        }
        if ($column->isUniqueKey()) {
            $nonDatatypeMetadata['uniqueKey'] = $column->isUniqueKey();
        }

        // Foreign key
        if ($column->hasForeignKey()) {
            $fk = $column->getForeignKey();
            $nonDatatypeMetadata['foreignKey'] = true;
            $nonDatatypeMetadata['foreignKeyName'] = $fk->hasName() ? $fk->getName() : null;
            $nonDatatypeMetadata['foreignKeyRefSchema'] = $fk->hasRefSchema() ? $fk->getRefSchema() : null;
            $nonDatatypeMetadata['foreignKeyRefTable'] = $fk->getRefTable();
            $nonDatatypeMetadata['foreignKeyRefColumn'] = $fk->getRefColumn();
        }

        foreach ($nonDatatypeMetadata as $key => $value) {
            if ($value === null) {
                // Skip null value
                continue;
            }

            $columnMetadata[] = [
                'key' => 'KBC.' . $key,
                'value' => $value,
            ];
        }

        // Constraints
        foreach ($column->getConstraints() as $constraint) {
            $columnMetadata[] = [
                'key' => 'KBC.constraintName',
                'value' => $constraint,
            ];
        }

        return $columnMetadata;
    }
}
