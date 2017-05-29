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
			$params['database']//,
		);

		$pdo = new \PDO($dsn, $params['user'], $params['password'], $options);
		$pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

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

	public function listTables()
    {
        // $tables = $this->db->query('SELECT Distinct TABLE_NAME FROM information_schema.TABLES');
        $stmt = $this->db->query("SELECT * FROM sysobjects WHERE xtype='U'");
        $tables = $stmt->fetchAll();
        $output = [];
        foreach ($tables as $table) {
            $output[] = $table['name'];
        }
        return $output;
    }

    public function describeTable($tableName)
    {
        $res = $this->db->query(sprintf("select *
                                    from information_schema.columns 
                                     where table_name = %s
                                     order by ordinal_position", $this->db->quote($tableName)));

        $res2 = $this->db->query(sprintf(
            "SELECT c.column_name as column_name, c.*, cc2.CONSTRAINT_TYPE
            FROM information_schema.columns as c 
            LEFT JOIN (
                SELECT tc.CONSTRAINT_TYPE, tc.table_name, ccu.column_name FROM information_schema.constraint_column_usage as ccu
                JOIN information_schema.table_constraints as tc
                ON ccu.table_name = tc.table_name
            ) as cc2 
            ON cc2.table_name = c.table_name AND cc2.column_name = c.column_name 
            WHERE c.table_name = %s
            ORDER BY ordinal_position", $this->db->quote($tableName)));

        $columns = [];

        while ($column = $res2->fetch(\PDO::FETCH_ASSOC)) {
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
                "primaryKey" => ($column['CONSTRAINT_TYPE'] === "PRIMARY KEY") ? true : false,
                "uniqueKey" => ($column['CONSTRAINT_TYPE'] === "UNIQUE") ? true : false,
                "foreignKey" => ($column['CONSTRAINT_TYPE'] === "FOREIGN KEY") ? true : false,
                "length" => $length,
                "nullable" => ($column['IS_NULLABLE'] === "YES") ? true : false,
                "default" => $column['COLUMN_DEFAULT'],
                "ordinalPosition" => $column['ORDINAL_POSITION']
            ];
        }
        return $columns;
    }
}
