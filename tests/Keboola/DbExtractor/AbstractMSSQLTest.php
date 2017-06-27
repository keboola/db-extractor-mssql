<?php

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\Csv\CsvFile;
use Symfony\Component\Yaml\Yaml;

abstract class AbstractMSSQLTest extends ExtractorTest
{
    /**
     * @var \PDO
     */
    protected $pdo;

    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-mssql');
        }

        $config = $this->getConfig('mssql');
        $dbConfig = $config['parameters']['db'];

        $dsn = sprintf(
            "dblib:host=%s:%d;dbname=%s;charset=UTF-8",
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
        $config = Yaml::parse(file_get_contents($this->dataDir . '/' .$driver . '/config.yml'));
        $config['parameters']['data_dir'] = $this->dataDir;

        $config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
        $config['parameters']['db']['password'] = $this->getEnv($driver, 'DB_PASSWORD');
        $config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');

        $config['parameters']['extractor_class'] = 'MSSQL';
        return $config;
    }

    /**
     * @param CsvFile $file
     * @return string
     */
    protected function generateTableName(CsvFile $file)
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
    protected function createTextTable(CsvFile $file, $primaryKey = null)
    {
        $tableName = $this->generateTableName($file);

        $this->pdo->exec(sprintf(
            'IF OBJECT_ID(\'%s\', \'U\') IS NOT NULL DROP TABLE %s',
            $tableName,
            $tableName
        ));

        $sql = sprintf(
            'CREATE TABLE %s (%s)',
            $tableName,
            implode(
                ', ',
                array_map(function ($column) {
                    return $column . ' varchar(255) NULL';
                }, $file->getHeader())
            ),
            $tableName
        );

        $this->pdo->exec($sql);

        // create the primary key if supplied
        if ($primaryKey && is_array($primaryKey) && !empty($primaryKey)) {

            foreach ($primaryKey as $pk) {
                $sql = sprintf("ALTER TABLE %s ALTER COLUMN %s varchar(64) NOT NULL", $tableName, $pk);
                $this->pdo->exec($sql);
            }

            $sql = sprintf(
                'ALTER TABLE %s ADD PRIMARY KEY (%s)',
                $tableName,
                implode(',', $primaryKey)
            );
            $this->pdo->exec($sql);
        }

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

    /**
     * @param array $config
     * @return MSSQLApplication
     */
    public function createApplication(array $config)
    {
        $app = new MSSQLApplication($config, $this->dataDir);

        return $app;
    }
}