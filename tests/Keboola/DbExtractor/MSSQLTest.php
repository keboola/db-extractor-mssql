<?php
/**
 * @package ex-db-mssql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Configuration\MSSSQLConfigDefinition;
use Keboola\DbExtractor\Test\ExtractorTest;

class MSSQLTest extends ExtractorTest
{
	/** @var Application */
	protected $app;

	/**
	 * @var \PDO
	 */
	protected $pdo;

	public function setUp()
	{
		define('APP_NAME', 'ex-db-mssql');
		$app = new Application($this->getConfig('mssql'));
		$app->setConfigDefinition(new MSSSQLConfigDefinition());
		$this->app = $app;

		$config = $this->getConfig('mssql');
		$dbConfig = $config['parameters']['db'];

		$dsn = sprintf(
			"dblib:host=%s;port=%d;dbname=%s;charset=UTF-8",
			$dbConfig['host'],
			$dbConfig['port'],
			$dbConfig['database']
		);

		$this->pdo = new \PDO($dsn, $dbConfig['user'], $dbConfig['password']);
		$this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
	}

	/**
	 * @param string $driver
	 * @return mixed
	 */
	public function getConfig($driver = 'mssql')
	{
		$config = parent::getConfig($driver);
		$config['extractor_class'] = 'MSSQL';
		return $config;
	}

	/**
	 * @param CsvFile $file
	 * @return string
	 */
	private function generateTableName(CsvFile $file)
	{
		$tableName = sprintf(
			'%s',
			$file->getBasename('.' . $file->getExtension())
		);

		return 'dbo.' . $tableName;
	}

	/**
	 * Create table from csv file with text columns
	 *
	 * @param CsvFile $file
	 */
	private function createTextTable(CsvFile $file)
	{
		$tableName = $this->generateTableName($file);

		$this->pdo->exec(sprintf(
			'IF OBJECT_ID(\'%s\', \'U\') IS NOT NULL DROP TABLE %s',
			$tableName,
			$tableName
		));

		$this->pdo->exec(sprintf(
			'CREATE TABLE %s (%s)',
			$tableName,
			implode(
				', ',
				array_map(function ($column) {
					return $column . ' text NULL';
				}, $file->getHeader())
			),
			$tableName
		));

		$file->next();

		$this->pdo->beginTransaction();

		$columnsCount = count($file->current());
		$rowsPerInsert = intval((1000 / $columnsCount) - 1);


		while ($file->current() !== false) {
			$sqlInserts = "";

			for ($i=0; $i<$rowsPerInsert && $file->current() !== false; $i++) {
				$sqlInserts = "";

				$sqlInserts .= sprintf(
					"(%s),",
					implode(
						',',
						array_map(function ($data) {
							if ($data == "") return 'null';
							if (is_numeric($data)) return "'" . $data . "'";

							$nonDisplayables = array(
								'/%0[0-8bcef]/',            // url encoded 00-08, 11, 12, 14, 15
								'/%1[0-9a-f]/',             // url encoded 16-31
								'/[\x00-\x08]/',            // 00-08
								'/\x0b/',                   // 11
								'/\x0c/',                   // 12
								'/[\x0e-\x1f]/'             // 14-31
							);
							foreach ($nonDisplayables as $regex) {
								$data = preg_replace($regex, '', $data);
							}

							$data = str_replace("'", "''", $data );

							return "'" . $data . "'";
						}, $file->current())
					)
				);
				$file->next();

				$sql = sprintf('INSERT INTO %s VALUES %s',
					$tableName,
					substr($sqlInserts, 0, -1)
				);

				$this->pdo->exec($sql);
			}

//			if ($sqlInserts) {
//				$sql = sprintf('INSERT INTO %s VALUES %s',
//					$tableName,
//					substr($sqlInserts, 0, -1)
//				);
//
//				$this->pdo->exec($sql);
//			}
		}

		$this->pdo->commit();

		$count = $this->pdo->query(sprintf('SELECT COUNT(*) AS itemsCount FROM %s', $tableName))->fetchColumn();
		$this->assertEquals($this->countTable($file), (int) $count);
	}

	/**
	 * Count records in CSV (with headers)
	 *
	 * @param CsvFile $file
	 * @return int
	 */
	protected function countTable(CsvFile $file)
	{
		$linesCount = 0;
		foreach ($file AS $i => $line)
		{
			// skip header
			if (!$i) {
				continue;
			}

			$linesCount++;
		}

		return $linesCount;
	}

	public function testRun()
	{
		$csv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');
		$this->createTextTable($csv1);

//		$csv2 = new CsvFile($this->dataDir . '/mssql/escaping.csv');
//		$this->createTextTable($csv2);

		$result = $this->app->run();

		$outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';

		$this->assertEquals('ok', $result['status']);
		$this->assertFileExists($outputCsvFile);
		$this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
		$this->assertFileEquals((string) $csv1, $outputCsvFile);


//		$outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][1] . '.csv';
//
//		$this->assertEquals('ok', $result['status']);
//		$this->assertFileExists($outputCsvFile);
//		$this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][1] . '.csv.manifest');
//		$this->assertFileEquals((string) $csv2, $outputCsvFile);
	}

}
