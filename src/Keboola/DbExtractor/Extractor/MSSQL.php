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
                WHERE (so.xtype='U' OR so.xtype='V') AND so.name NOT IN ('sysconstraints', 'syssegments')"; // xtype='U' user generated objects only

        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                " AND so.name IN (%s)",
                implode(',', array_map(function ($table) {
                    return $this->db->quote($table);
                }, $tables))
            );
        }

        $stmt = $this->db->query($sql);

        $arr = $stmt->fetchAll();
        $output = [];
        foreach ($arr as $table) {
            $output[] = $this->describeTable($table);
        }
        return $output;
    }

    public function describeTable(array $table)
    {
        $tabledef = [
            'name' => $table['TABLE_NAME'],
            'catalog' => (isset($table['TABLE_CATALOG'])) ? $table['TABLE_CATALOG'] : null,
            'schema' => (isset($table['TABLE_SCHEMA'])) ? $table['TABLE_SCHEMA'] : null,
            'type' => (isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : null
        ];

        $res = $this->db->query(sprintf(
            "SELECT c.column_name AS column_name, c.*, 
              cc2.CONSTRAINT_TYPE, cc2.CONSTRAINT_NAME,
              FK_REFS.REFERENCED_COLUMN_NAME, 
              FK_REFS.REFERENCED_TABLE_NAME,
              FK_REFS.REFERENCED_SCHEMA_NAME
            FROM information_schema.columns AS c 
            LEFT JOIN (
                SELECT tc.CONSTRAINT_TYPE, tc.table_name, ccu.column_name, ccu.CONSTRAINT_NAME
                FROM information_schema.constraint_column_usage AS ccu
                JOIN information_schema.table_constraints AS tc
                ON ccu.table_name = tc.table_name
            ) AS cc2 
            ON cc2.table_name = c.table_name AND cc2.column_name = c.column_name
            LEFT JOIN (
                SELECT  
                     KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME 
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
            ON FK_REFS.FK_CONSTRAINT_NAME = cc2.CONSTRAINT_NAME
            WHERE c.table_name = %s
            ORDER BY ordinal_position", $this->db->quote($table['TABLE_NAME'])));

        $columns = [];

        $rows = $res->fetchAll();
        foreach ($rows as $i => $column) {
            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if ($column['NUMERIC_SCALE'] > 0) {
                    $length = $column['NUMERIC_PRECISION'] . "," . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }
            $columns[] = [
                "name" => $column['column_name'],
                "type" => $column['DATA_TYPE'],
                "length" => $length,
                "nullable" => ($column['IS_NULLABLE'] === "YES") ? true : false,
                "default" => $column['COLUMN_DEFAULT'],
                "ordinalPosition" => $column['ORDINAL_POSITION'],
                "primaryKey" => ($column['CONSTRAINT_TYPE'] === "PRIMARY KEY") ? true : false,
                "uniqueKey" => ($column['CONSTRAINT_TYPE'] === "UNIQUE") ? true : false,
                "foreignKey" => ($column['CONSTRAINT_TYPE'] === "FOREIGN KEY") ? true : false
            ];

            if ($column['CONSTRAINT_TYPE'] !== null) {
                $columns[$i]['constraintName'] = $column['CONSTRAINT_NAME'];
                if ($column['CONSTRAINT_TYPE'] === 'FOREIGN KEY') {
                    $columns[$i]['foreignKeyRefSchema'] = $column['REFERENCED_SCHEMA_NAME'];
                    $columns[$i]['foreignKeyRefTable'] = $column['REFERENCED_TABLE_NAME'];
                    $columns[$i]['foreignKeyRefColumn'] = $column['REFERENCED_COLUMN_NAME'];
                }
            }
        }
        $tabledef['columns'] = $columns;

        return $tabledef;
    }

    public function simpleQuery($table, $columns = array())
    {
        if (count($columns) > 0) {
            return sprintf("SELECT %s FROM %s",
                implode(', ', array_map(function ($column) {
                    return $this->quote($column);
                }, $columns)),
                $this->quote($table)
            );
        } else {
            return sprintf("SELECT * FROM %s", $this->quote($table));
        }
    }

    private function quote($obj)
    {
        return "\"{$obj}\"";
    }
}
