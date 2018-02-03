<?php
/**
 * @package ex-db-mssql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\UserException;

class MSSQL extends Extractor
{
    public function createConnection($params)
    {
        // convert errors to PDOExceptions
        $options = [
         \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
         \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC
        ];

        // check params
        if (isset($params['#password'])) {
                $params['password'] = $params['#password'];
        }
        
        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!array_key_exists($r, $params)) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($params['port']) ? $params['port'] : '1433';
        $dsn = sprintf(
            "dblib:host=%s:%d;dbname=%s;charset=UTF-8",
            $params['host'],
            $port,
            $params['database']
        );

        $pdo = new \PDO($dsn, $params['user'], $params['password'], $options);

        return $pdo;
    }

    public function getConnection()
    {
        return $this->db;
    }

    public function testConnection()
    {
        $this->db->query('SELECT GETDATE() AS CurrentDateTime')->execute();
    }

    public function getTables(array $tables = null)
    {
        $sql = "SELECT ist.* FROM information_schema.tables as ist
                INNER JOIN sysobjects AS so ON ist.TABLE_NAME = so.name
                WHERE (so.xtype='U' OR so.xtype='V') AND so.name NOT IN ('sysconstraints', 'syssegments')";
                // xtype='U' user generated objects only

        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                " AND TABLE_NAME IN (%s) AND TABLE_SCHEMA IN (%s)",
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

        $sql .= " ORDER BY TABLE_SCHEMA, TABLE_NAME";

        $stmt = $this->db->query($sql);

        $arr = $stmt->fetchAll();
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
                'type' => (isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : ''
            ];
        }

        if (count($tableNameArray) === 0) {
            return [];
        }

        if ($tables === null || count($tables) === 0) {
            $sql = $this->quickTablesSql();
        } else {
            $sql = $this->fullTablesSql($tables);
        }


        $res = $this->db->query($sql);
        $rows = $res->fetchAll();
        foreach ($rows as $i => $column) {
            $curTable = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'];
            if (!array_key_exists('columns', $tableDefs[$curTable])) {
                $tableDefs[$curTable]['columns'] = [];
            }
            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if ($column['NUMERIC_SCALE'] > 0) {
                    $length = $column['NUMERIC_PRECISION'] . "," . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }
            $curColumnIndex = $column['ORDINAL_POSITION'] - 1;
            if (!array_key_exists($curColumnIndex, $tableDefs[$curTable]['columns'])) {
                $tableDefs[$curTable]['columns'][$curColumnIndex] = [
                    "name" => $column['COLUMN_NAME'],
                    "type" => $column['DATA_TYPE'],
                    "length" => $length,
                    "nullable" => ($column['IS_NULLABLE'] === "YES") ? true : false,
                    "default" => $column['COLUMN_DEFAULT'],
                    "ordinalPosition" => $column['ORDINAL_POSITION'],
                    "primaryKey" => false,
                ];
            }

            if (array_key_exists('pk_name', $column) && $column['pk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['primaryKey'] = true;
                $tableDefs[$curTable]['columns'][$curColumnIndex]['primaryKeyName'] = $column['pk_name'];
            }
            if (array_key_exists('uk_name', $column) && $column['uk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['uniqueKey'] = true;
                $tableDefs[$curTable]['columns'][$curColumnIndex]['uniqueKeyName'] = $column['uk_name'];
            }
            if (array_key_exists('chk_name', $column) && $column['chk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]["checkConstraint"] = $column['chk_name'];
                if (isset($column['CHECK_CLAUSE']) && $column['CHECK_CLAUSE'] !== null) {
                    $tableDefs[$curTable]['columns'][$curColumnIndex]["checkClause"] = $column['CHECK_CLAUSE'];
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

    private function quickTablesSql()
    {
        return "SELECT c.*, pk_name 
                FROM information_schema.columns AS c
                INNER JOIN sysobjects AS so ON c.TABLE_NAME = so.name AND (so.xtype='U' OR so.xtype='V') AND so.name NOT IN ('sysconstraints', 'syssegments')
                LEFT JOIN (
                    SELECT tc.CONSTRAINT_TYPE, tc.table_name, ccu.column_name, ccu.CONSTRAINT_NAME as pk_name
                    FROM information_schema.key_column_usage AS ccu
                    JOIN information_schema.table_constraints AS tc
                    ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND  ccu.table_name = tc.table_name AND CONSTRAINT_TYPE = 'PRIMARY KEY' 
                ) AS pk
                ON pk.table_name = c.table_name AND pk.column_name = c.column_name";
    }

    private function fullTablesSql($tables) {
        return sprintf(
            "SELECT c.*,  
              chk.CHECK_CLAUSE, 
              fk_name,
              chk_name,
              pk_name,
              uk_name,
              FK_REFS.REFERENCED_COLUMN_NAME, 
              FK_REFS.REFERENCED_TABLE_NAME,
              FK_REFS.REFERENCED_SCHEMA_NAME
            FROM information_schema.columns AS c 
            LEFT JOIN (
                SELECT  
                     KCU1.CONSTRAINT_NAME AS fk_name 
                    ,KCU1.CONSTRAINT_SCHEMA AS FK_SCHEMA_NAME
                    ,KCU1.TABLE_NAME AS FK_TABLE_NAME 
                    ,KCU1.COLUMN_NAME AS FK_COLUMN_NAME 
                    ,KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION 
                    ,KCU2.CONSTRAINT_NAME AS REFERENCED_CONSTRAINT_NAME 
                    ,KCU2.CONSTRAINT_SCHEMA AS REFERENCED_SCHEMA_NAME
                    ,KCU2.TABLE_NAME AS REFERENCED_TABLE_NAME 
                    ,KCU2.COLUMN_NAME AS REFERENCED_COLUMN_NAME 
                    ,KCU2.ORDINAL_POSITION AS REFERENCED_ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC 
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1 
                    ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
                    AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
                    AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2 
                    ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG  
                    AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA 
                    AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME 
                    AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION 
            ) AS FK_REFS
            ON FK_REFS.FK_TABLE_NAME = c.table_name AND FK_REFS.FK_COLUMN_NAME = c.column_name
            LEFT JOIN (
                SELECT tc2.CONSTRAINT_TYPE, tc2.table_name, ccu2.column_name, ccu2.CONSTRAINT_NAME as chk_name, CHK.CHECK_CLAUSE 
                FROM information_schema.constraint_column_usage AS ccu2 
                JOIN information_schema.table_constraints AS tc2 
                ON ccu2.table_name = tc2.table_name
                JOIN (
                  SELECT * FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS 
                ) AS CHK 
                ON CHK.CONSTRAINT_NAME = ccu2.CONSTRAINT_NAME
                WHERE CONSTRAINT_TYPE = 'CHECK'
            ) AS chk
            ON chk.table_name = c.table_name AND chk.column_name = c.column_name
            LEFT JOIN (
                SELECT tc.CONSTRAINT_TYPE, tc.table_name, ccu.column_name, ccu.CONSTRAINT_NAME as pk_name
                FROM information_schema.key_column_usage AS ccu
                JOIN information_schema.table_constraints AS tc
                ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND  ccu.table_name = tc.table_name AND CONSTRAINT_TYPE = 'PRIMARY KEY' 
            ) AS pk
            ON pk.table_name = c.table_name AND pk.column_name = c.column_name
            LEFT JOIN (
                SELECT tc.CONSTRAINT_TYPE, ccu.table_name, ccu.column_name, ccu.CONSTRAINT_NAME as uk_name
                FROM information_schema.key_column_usage AS ccu
                JOIN information_schema.table_constraints AS tc
                ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND ccu.table_name = tc.table_name AND CONSTRAINT_TYPE = 'UNIQUE' 
            ) AS uk  
            ON uk.table_name = c.table_name AND uk.column_name = c.column_name
            WHERE c.table_name IN (%s)
            ORDER BY c.table_schema, c.table_name, ordinal_position",
            implode(
                ',',
                array_map(
                    function ($table) {
                        return $this->db->quote($table['tableName']);
                    },
                    $tables
                )
            )
        );
    }

    public function simpleQuery(array $table, array $columns = array())
    {
        if (count($columns) > 0) {
            return sprintf(
                "SELECT %s FROM %s.%s",
                implode(
                    ', ',
                    array_map(
                        function ($column) {
                            return $this->quote($column);
                        },
                        $columns
                    )
                ),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            return sprintf(
                "SELECT * FROM %s.%s",
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }
    }

    private function quote($obj)
    {
        return "\"{$obj}\"";
    }
}
