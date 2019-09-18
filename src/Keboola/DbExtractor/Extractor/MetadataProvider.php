<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Extractor\DbAdapter\MssqlAdapter;

class MetadataProvider
{
    /** @var MssqlAdapter */
    private $db;

    public function __construct(
        MssqlAdapter $db
    ) {
        $this->db = $db;
    }

    private function fullTablesSql(array $tables): string
    {
        return sprintf(
            "SELECT [c].*,  
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
            ORDER BY [c].[TABLE_SCHEMA], [c].[TABLE_NAME], [ORDINAL_POSITION]",
            implode(
                ',',
                array_map(
                    function ($table) {
                        return $this->db->quote($table['tableName']);
                    },
                    $tables
                )
            ),
            implode(
                ',',
                array_map(
                    function ($table) {
                        return $this->db->quote($table['schema']);
                    },
                    $tables
                )
            )
        );
    }

    private function getFieldLength(array $column): ?string
    {
        $dateTimeTypes = ['datetimeoffset', 'datetime2', 'datetime', 'time', 'smalldatetime', 'date'];
        if (in_array($column['DATA_TYPE'], $dateTimeTypes)) {
            return null;
        }
        if ($column['NUMERIC_PRECISION'] > 0) {
            if ($column['NUMERIC_SCALE'] > 0) {
                return $column['NUMERIC_PRECISION'] . ',' . $column['NUMERIC_SCALE'];
            } else {
                return $column['NUMERIC_PRECISION'];
            }
        }
        switch ($column['CHARACTER_MAXIMUM_LENGTH']) {
            case '16':
                // most likely TEXT column
                if ($column['DATA_TYPE'] === 'text') {
                    return null;
                } else {
                    return $column['CHARACTER_MAXIMUM_LENGTH'];
                }
            case '-1':
                // this is returned for max, ex: nvarchar(max), we will treat it as unspecified
                return null;
            default:
                return $column['CHARACTER_MAXIMUM_LENGTH'];
                break;
        }
    }

    private function quickTablesSql(): string
    {
        return "SELECT 
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
              ";
    }

    public function getTables(?array $tables = null): array
    {
        $sql = "SELECT [ist].* FROM [INFORMATION_SCHEMA].[TABLES] as [ist]
                INNER JOIN [sys].[objects] AS [so] ON [ist].[TABLE_NAME] = [so].[name]
                WHERE ([so].[type]='U' OR [so].[type]='V')";
        // xtype='U' user generated objects only

        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                ' AND TABLE_NAME IN (%s) AND TABLE_SCHEMA IN (%s)',
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->db->quote($table['tableName']);
                        },
                        $tables
                    )
                ),
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->db->quote($table['schema']);
                        },
                        $tables
                    )
                )
            );
        }
        $stmt = $this->db->query($sql);

        $arr = (array) $stmt->fetchAll();
        if (count($arr) === 0) {
            return [];
        }

        $tableNameArray = [];
        $tableDefs = [];
        foreach ($arr as $table) {
            $tableNameArray[] = $table['TABLE_NAME'];
            $tableDefs[$table['TABLE_SCHEMA'] . '.' . $table['TABLE_NAME']] = [
                'name' => $table['TABLE_NAME'],
                'catalog' => (isset($table['TABLE_CATALOG'])) ? $table['TABLE_CATALOG'] : '',
                'schema' => (isset($table['TABLE_SCHEMA'])) ? $table['TABLE_SCHEMA'] : '',
                'type' => (isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : '',
                'columns' => [],
            ];
        }
        ksort($tableDefs);

        if (count($tableNameArray) === 0) {
            return [];
        }

        if ($tables === null || count($tables) === 0) {
            $sql = $this->quickTablesSql();
        } else {
            $sql = $this->fullTablesSql($tables);
        }

        $res = $this->db->query($sql);

        $rows = (array) $res->fetchAll();

        foreach ($rows as $i => $column) {
            $curTable = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'];
            if (!array_key_exists('columns', $tableDefs[$curTable])) {
                $tableDefs[$curTable]['columns'] = [];
            }

            $curColumnIndex = $column['ORDINAL_POSITION'] - 1;
            if (!array_key_exists($curColumnIndex, $tableDefs[$curTable]['columns'])) {
                $tableDefs[$curTable]['columns'][$curColumnIndex] = [
                    'name' => $column['COLUMN_NAME'],
                    'sanitizedName' => \Keboola\Utils\sanitizeColumnName($column['COLUMN_NAME']),
                    'type' => $column['DATA_TYPE'],
                    'length' => $this->getFieldLength($column),
                    'nullable' => ($column['IS_NULLABLE'] === 'YES' || $column['IS_NULLABLE'] === '1') ? true : false,
                    'ordinalPosition' => (int) $column['ORDINAL_POSITION'],
                    'primaryKey' => false,
                ];
            }

            if (array_key_exists('COLUMN_DEFAULT', $column)) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['default'] = $column['COLUMN_DEFAULT'];
            }

            if (array_key_exists('pk_name', $column) && $column['pk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['primaryKey'] = true;
                $tableDefs[$curTable]['columns'][$curColumnIndex]['primaryKeyName'] = $column['pk_name'];
            }
            if (array_key_exists('is_identity', $column) && $column['is_identity']) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['autoIncrement'] = true;
            }
            if (array_key_exists('uk_name', $column) && $column['uk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['uniqueKey'] = true;
                $tableDefs[$curTable]['columns'][$curColumnIndex]['uniqueKeyName'] = $column['uk_name'];
            }
            if (array_key_exists('chk_name', $column) && $column['chk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['checkConstraint'] = $column['chk_name'];
                if (isset($column['CHECK_CLAUSE']) && $column['CHECK_CLAUSE'] !== null) {
                    $tableDefs[$curTable]['columns'][$curColumnIndex]['checkClause'] = $column['CHECK_CLAUSE'];
                }
            }
            if (array_key_exists('fk_name', $column) && $column['fk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['foreignKey'] = true;
                $tableDefs[$curTable]['columns'][$curColumnIndex]['foreignKeyName'] = $column['fk_name'];
                $tableDefs[$curTable]['columns'][$curColumnIndex]['foreignKeyRefSchema'] = $column['REFERENCED_SCHEMA_NAME'];
                $tableDefs[$curTable]['columns'][$curColumnIndex]['foreignKeyRefTable'] = $column['REFERENCED_TABLE_NAME'];
                $tableDefs[$curTable]['columns'][$curColumnIndex]['foreignKeyRefColumn'] = $column['REFERENCED_COLUMN_NAME'];
            }
        }
        return array_values($tableDefs);
    }
}
