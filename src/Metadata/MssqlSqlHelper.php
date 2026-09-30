<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata;

use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

class MssqlSqlHelper
{
    public const DATE_TIME_TYPES = ['datetimeoffset', 'datetime2', 'datetime', 'time', 'smalldatetime', 'date'];

    /**
     * @param array|InputTable[] $whitelist
     */
    public static function getTablesSql(array $whitelist, MSSQLPdoConnection $pdo): string
    {
        // Note: type='U' user generated objects only
        $sql = [];
        $sql[] = "
            SELECT [ist].*, [st].is_tracked_by_cdc FROM [INFORMATION_SCHEMA].[TABLES] as [ist]
            INNER JOIN [sys].[objects] AS [so] ON [ist].[TABLE_NAME] = [so].[name]
            LEFT JOIN [sys].[tables] AS [st] ON [so].[object_id] = [st].[object_id]
            WHERE ([so].[type]='U' OR [so].[type]='V') AND [so].[is_ms_shipped] = 0
        ";

        if (!empty($whitelist)) {
            $sql[] = sprintf(
                'AND TABLE_NAME IN (%s) AND TABLE_SCHEMA IN (%s)',
                implode(',', array_map(
                    fn (InputTable $table) => $pdo->quote($table->getName()),
                    $whitelist,
                )),
                implode(',', array_map(
                    fn (InputTable $table) => $pdo->quote($table->getSchema()),
                    $whitelist,
                )),
            );
        }

        $sql[] = 'ORDER BY [TABLE_SCHEMA], [TABLE_NAME]';

        return implode(' ', $sql);
    }

    public static function getColumnsSqlQuick(): string
    {
        return "
            SELECT 
              OBJECT_SCHEMA_NAME ([sys].[columns].[object_id]) AS [TABLE_SCHEMA],
              OBJECT_NAME([sys].[columns].[object_id]) as [TABLE_NAME],
              [sys].[columns].[column_id] AS [COLUMN_ID],
              [sys].[columns].[column_id] AS [ORDINAL_POSITION],
              [sys].[columns].[name] AS [COLUMN_NAME],
              TYPE_NAME([sys].[columns].[system_type_id]) AS [DATA_TYPE],
              [sys].[columns].[is_nullable] AS [IS_NULLABLE],
              [sys].[columns].[precision] AS [NUMERIC_PRECISION],
              [sys].[columns].[scale] AS [NUMERIC_SCALE],
              [sys].[columns].[max_length] AS [CHARACTER_MAXIMUM_LENGTH],
              [pks].[index_name] AS [pk_name],
              [pks].[is_identity] AS [is_identity]
            FROM [sys].[columns] 
            LEFT JOIN
              (
                SELECT [i].[name] AS [index_name],
                    [is_identity],
                    [c].[column_id] AS [columnid],
                    [c].[object_id] AS [objectid]
                FROM [sys].[indexes] AS [i]  
                INNER JOIN [sys].[index_columns] AS [ic]   
                    ON [i].[object_id] = [ic].[object_id] AND [i].[index_id] = [ic].[index_id]  
                INNER JOIN [sys].[columns] AS [c]   
                    ON [ic].[object_id] = [c].[object_id] AND [c].[column_id] = [ic].[column_id]  
                WHERE [i].[is_primary_key] = 1
              ) [pks] 
            ON [pks].[objectid] = [sys].[columns].[object_id] AND [pks].[columnid] = [sys].[columns].[column_id]
            INNER JOIN [sys].[objects] AS [so] ON [sys].[columns].[object_id] = [so].[object_id]
            WHERE ([so].[type]='U' OR [so].[type]='V') AND [so].[is_ms_shipped] = 0
            ORDER BY [TABLE_SCHEMA], [TABLE_NAME], [ORDINAL_POSITION]
        ";
    }

    /**
     * @param array|InputTable[] $whitelist
     */
    public static function getColumnsSqlComplex(array $whitelist, MSSQLPdoConnection $pdo): string
    {
        // @codingStandardsIgnoreStart
        $sql = "
            SELECT [c].*,  
              [chk].[CHECK_CLAUSE], 
              [fk_name],
              [chk_name],
              [pk_name],
              [uk_name],
              [FK_REFS].[REFERENCED_COLUMN_NAME], 
              [FK_REFS].[REFERENCED_TABLE_NAME],
              [FK_REFS].[REFERENCED_SCHEMA_NAME]
            FROM [INFORMATION_SCHEMA].[COLUMNS] AS [c] 
            LEFT JOIN (
                SELECT  
                     [KCU1].[CONSTRAINT_NAME] AS [fk_name] 
                    ,[KCU1].[CONSTRAINT_SCHEMA] AS [FK_SCHEMA_NAME]
                    ,[KCU1].[TABLE_NAME] AS [FK_TABLE_NAME] 
                    ,[KCU1].[COLUMN_NAME] AS [FK_COLUMN_NAME] 
                    ,[KCU1].[ORDINAL_POSITION] AS [FK_ORDINAL_POSITION] 
                    ,[KCU2].[CONSTRAINT_NAME] AS [REFERENCED_CONSTRAINT_NAME] 
                    ,[KCU2].[CONSTRAINT_SCHEMA] AS [REFERENCED_SCHEMA_NAME]
                    ,[KCU2].[TABLE_NAME] AS [REFERENCED_TABLE_NAME] 
                    ,[KCU2].[COLUMN_NAME] AS [REFERENCED_COLUMN_NAME] 
                    ,[KCU2].[ORDINAL_POSITION] AS [REFERENCED_ORDINAL_POSITION]
                FROM [INFORMATION_SCHEMA].[REFERENTIAL_CONSTRAINTS] AS [RC] 
                INNER JOIN [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] AS [KCU1] 
                    ON [KCU1].[CONSTRAINT_CATALOG] = [RC].[CONSTRAINT_CATALOG]  
                    AND [KCU1].[CONSTRAINT_SCHEMA] = [RC].[CONSTRAINT_SCHEMA] 
                    AND [KCU1].[CONSTRAINT_NAME] = [RC].[CONSTRAINT_NAME] 
                INNER JOIN [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] AS [KCU2] 
                    ON [KCU2].[CONSTRAINT_CATALOG] = [RC].[UNIQUE_CONSTRAINT_CATALOG]  
                    AND [KCU2].[CONSTRAINT_SCHEMA] = [RC].[UNIQUE_CONSTRAINT_SCHEMA] 
                    AND [KCU2].[CONSTRAINT_NAME] = [RC].[UNIQUE_CONSTRAINT_NAME] 
                    AND [KCU2].[ORDINAL_POSITION] = [KCU1].[ORDINAL_POSITION] 
            ) AS [FK_REFS]
            ON [FK_REFS].[FK_TABLE_NAME] = [c].[TABLE_NAME] AND [FK_REFS].[FK_COLUMN_NAME] = [c].[COLUMN_NAME]
            LEFT JOIN (
                SELECT [tc2].[CONSTRAINT_TYPE], [tc2].[TABLE_NAME], [ccu2].[COLUMN_NAME], [ccu2].[CONSTRAINT_NAME] as [chk_name], [CHK].[CHECK_CLAUSE] 
                FROM [INFORMATION_SCHEMA].[CONSTRAINT_COLUMN_USAGE] AS [ccu2] 
                JOIN [INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] AS [tc2] 
                ON [ccu2].[TABLE_NAME] = [tc2].[TABLE_NAME]
                JOIN (
                  SELECT * FROM [INFORMATION_SCHEMA].[CHECK_CONSTRAINTS] 
                ) AS [CHK] 
                ON [CHK].[CONSTRAINT_NAME] = [ccu2].[CONSTRAINT_NAME]
                WHERE [CONSTRAINT_TYPE] = 'CHECK'
            ) AS [chk]
            ON [chk].[TABLE_NAME] = [c].[TABLE_NAME] AND [chk].[COLUMN_NAME] = [c].[COLUMN_NAME]
            LEFT JOIN (
                SELECT [tc].[CONSTRAINT_TYPE], [tc].[TABLE_NAME], [ccu].[COLUMN_NAME], [ccu].[CONSTRAINT_NAME] as [pk_name]
                FROM [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] AS [ccu]
                JOIN [INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] AS [tc]
                ON [ccu].[CONSTRAINT_NAME] = [tc].[CONSTRAINT_NAME] AND  [ccu].[TABLE_NAME] = [tc].[TABLE_NAME] AND [CONSTRAINT_TYPE] = 'PRIMARY KEY' 
            ) AS [pk]
            ON [pk].[TABLE_NAME] = [c].[TABLE_NAME] AND [pk].[COLUMN_NAME] = [c].[COLUMN_NAME]
            LEFT JOIN (
                SELECT [tc].[CONSTRAINT_TYPE], [ccu].[TABLE_NAME], [ccu].[COLUMN_NAME], [ccu].[CONSTRAINT_NAME] as [uk_name]
                FROM [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] AS [ccu]
                JOIN [INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] AS [tc]
                ON [ccu].[CONSTRAINT_NAME] = [tc].[CONSTRAINT_NAME] AND [ccu].[TABLE_NAME] = [tc].[TABLE_NAME] AND [CONSTRAINT_TYPE] = 'UNIQUE' 
            ) AS [uk]  
            ON [uk].[TABLE_NAME] = [c].[TABLE_NAME] AND [uk].[COLUMN_NAME] = [c].[COLUMN_NAME]
            WHERE [c].[TABLE_NAME] IN (%s) AND [c].[TABLE_SCHEMA] IN (%s)
            ORDER BY [c].[TABLE_SCHEMA], [c].[TABLE_NAME], [ORDINAL_POSITION]
        ";
        // @codingStandardsIgnoreEnd

        return sprintf(
            $sql,
            implode(',', array_map(
                fn(InputTable $table) => $pdo->quote($table->getName()),
                $whitelist,
            )),
            implode(',', array_map(
                fn (InputTable $table) => $pdo->quote($table->getSchema()),
                $whitelist,
            )),
        );
    }

    /**
     * Reads the `MS_Description` extended property of tables/views and of their columns.
     *
     * SQL Server has no COMMENT statement -- a description is an extended property stored in
     * `sys.extended_properties`, written by `sp_addextendedproperty` or by the "Description"
     * field in SSMS, both of which use the name `MS_Description`. Class 1 covers the object and
     * its columns at once: `minor_id` 0 is the object itself, a higher one is the column of that
     * `column_id`.
     *
     * Deliberately a query of its own rather than a join into the existing metadata queries.
     * getTablesSql() joins INFORMATION_SCHEMA.TABLES to sys.objects on the table NAME alone, and
     * getColumnsSqlComplex() joins its constraint subqueries on table and column NAME alone, so
     * both already return duplicate rows for a name living in several schemas, which the provider
     * collapses -- a description joined in there could end up on the wrong table. Keyed on
     * (schema, table) here, the existing queries stay byte-identical whether the propagation is
     * on or off, which MssqlMetadataProviderTest::testTheExistingQueriesAreUntouched() pins down.
     *
     * @param array|InputTable[] $whitelist
     */
    public static function getDescriptionsSql(array $whitelist, MSSQLPdoConnection $pdo, bool $loadColumns): string
    {
        $select = [
            '[s].[name] AS [TABLE_SCHEMA]',
            '[o].[name] AS [TABLE_NAME]',
            // The value is sql_variant, capped at 7500 bytes, so nvarchar(4000) cannot truncate it
            'CAST([ep].[value] AS NVARCHAR(4000)) AS [DESCRIPTION]',
        ];

        $from = [];
        $from[] = 'FROM [sys].[extended_properties] AS [ep]';
        $from[] = 'INNER JOIN [sys].[objects] AS [o] ON [o].[object_id] = [ep].[major_id]';
        $from[] = 'INNER JOIN [sys].[schemas] AS [s] ON [s].[schema_id] = [o].[schema_id]';

        $where = [];
        $where[] = "[ep].[class] = 1 AND [ep].[name] = 'MS_Description'";
        // Same object filter as getTablesSql(): user tables and views only
        $where[] = "([o].[type]='U' OR [o].[type]='V') AND [o].[is_ms_shipped] = 0";

        if ($loadColumns) {
            $select[] = '[c].[name] AS [COLUMN_NAME]';
            $from[] = 'LEFT JOIN [sys].[columns] AS [c] '
                . 'ON [c].[object_id] = [ep].[major_id] AND [c].[column_id] = [ep].[minor_id]';

            // Keeps minor_id 0 (the object itself) while dropping a property pointing at a
            // column that no longer exists, which would otherwise read as a table description
            $where[] = '([ep].[minor_id] = 0 OR [c].[name] IS NOT NULL)';
        } else {
            $where[] = '[ep].[minor_id] = 0';
        }

        if (!empty($whitelist)) {
            $where[] = sprintf(
                '[o].[name] IN (%s) AND [s].[name] IN (%s)',
                implode(',', array_map(
                    fn (InputTable $table) => $pdo->quote($table->getName()),
                    $whitelist,
                )),
                implode(',', array_map(
                    fn (InputTable $table) => $pdo->quote($table->getSchema()),
                    $whitelist,
                )),
            );
        }

        $sql = [];
        $sql[] = sprintf('SELECT %s', implode(', ', $select));
        $sql = array_merge($sql, $from);
        $sql[] = sprintf('WHERE %s', implode(' AND ', $where));
        // The table description (minor_id 0) sorts ahead of its columns, which only helps reading
        // a query log -- the provider builds a map, so the order does not matter to it
        $sql[] = 'ORDER BY [s].[name], [o].[name], [ep].[minor_id]';

        return implode(' ', $sql);
    }

    public static function getFieldLength(array $data): ?string
    {
        if (in_array($data['DATA_TYPE'], self::DATE_TIME_TYPES)) {
            return null;
        }

        if ($data['NUMERIC_PRECISION'] > 0) {
            if ($data['NUMERIC_SCALE'] > 0) {
                return $data['NUMERIC_PRECISION'] . ',' . $data['NUMERIC_SCALE'];
            } else {
                return $data['NUMERIC_PRECISION'];
            }
        }

        switch ($data['CHARACTER_MAXIMUM_LENGTH']) {
            case '16':
                // most likely TEXT column
                if ($data['DATA_TYPE'] === 'text') {
                    return null;
                } else {
                    return $data['CHARACTER_MAXIMUM_LENGTH'];
                }
            case '-1':
                // this is returned for max, ex: nvarchar(max), we will treat it as unspecified
                return null;
            default:
                return $data['CHARACTER_MAXIMUM_LENGTH'];
        }
    }



    public static function getDefaultValue(string $dataType, string $defaultValue): string
    {
        switch (strtolower($dataType)) {
            case 'int':
            case 'bigint':
            case 'smallint':
            case 'tinyint':
            case 'decimal':
            case 'numeric':
            case 'float':
                preg_match('/\(\((.+)\)\)/', $defaultValue, $match);
                if (isset($match[1])) {
                    return $match[1];
                }
                return $defaultValue;
            default:
                return $defaultValue;
        }
    }
}
