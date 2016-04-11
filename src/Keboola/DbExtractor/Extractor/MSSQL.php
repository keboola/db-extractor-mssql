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
}
