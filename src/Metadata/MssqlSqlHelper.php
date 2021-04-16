<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata;

use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

class MssqlSqlHelper
{
    /**
     * @param array|InputTable[] $whitelist
     */
    public static function getTablesSql(array $whitelist, MSSQLPdoConnection $pdo): string
    {
        // Note: type='U' user generated objects only
        $sql = [];
        $sql[] = "
            SELECT [ist].* FROM [INFORMATION_SCHEMA].[TABLES] as [ist]
            INNER JOIN [sys].[objects] AS [so] ON [ist].[TABLE_NAME] = [so].[name]
            WHERE ([so].[type]='U' OR [so].[type]='V')
        ";

        if (!empty($whitelist)) {
            $sql[] = sprintf(
                'AND TABLE_NAME IN (%s) AND TABLE_SCHEMA IN (%s)',
                implode(',', array_map(
                    fn (InputTable $table) => $pdo->quote($table->getName()),
                    $whitelist
                )),
                implode(',', array_map(
                    fn (InputTable $table) => $pdo->quote($table->getSchema()),
                    $whitelist
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
            WHERE ([so].[type]='U' OR [so].[type]='V')
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
                $whitelist
            )),
            implode(',', array_map(
                fn (InputTable $table) => $pdo->quote($table->getSchema()),
                $whitelist
            ))
        );
    }

    public static function getFieldLength(array $data): ?string
    {
        $dateTimeTypes = ['datetimeoffset', 'datetime2', 'datetime', 'time', 'smalldatetime', 'date'];
        if (in_array($data['DATA_TYPE'], $dateTimeTypes)) {
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
}
