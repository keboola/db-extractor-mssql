<?php

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\MSSQLApplication;
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

        if (!$this->pdo) {
            $this->makeConnection();
        }

        $this->setupTables();
    }

    private function makeConnection()
    {
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC
        ];

        $config = $this->getConfig('mssql');
        $dbConfig = $config['parameters']['db'];

        $dsn = sprintf(
            "sqlsrv:Server=%s,%d;Database=%s;",
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['database']
        );

        $this->pdo = new \PDO($dsn, $dbConfig['user'], $dbConfig['password'], $options);
    }

    private function setupTables()
    {
        $csv1 = new CsvFile($this->dataDir . "/mssql/sales.csv");

        $this->pdo->exec("IF OBJECT_ID('dbo.sales2', 'U') IS NOT NULL DROP TABLE dbo.sales2");
        $this->pdo->exec("IF OBJECT_ID('dbo.sales', 'U') IS NOT NULL DROP TABLE dbo.sales");
        $this->createTextTable($csv1, ['createdat'], "sales");
        $this->createTextTable($csv1, null, "sales2");
        // drop the t1 demo table if it exists
        $this->pdo->exec("IF OBJECT_ID('t1', 'U') IS NOT NULL DROP TABLE t1");

        // set up a foreign key relationship
        $this->pdo->exec("ALTER TABLE sales2 ALTER COLUMN createdat varchar(64) NOT NULL");
        $this->pdo->exec("ALTER TABLE sales2 ADD CONSTRAINT FK_sales_sales2 FOREIGN KEY (createdat) REFERENCES sales(createdat)");

        // create another table with an auto_increment ID
        $this->pdo->exec("IF OBJECT_ID('dbo.autoIncrement', 'U') IS NOT NULL DROP TABLE dbo.autoIncrement");

        $this->pdo->exec("CREATE TABLE autoIncrement (ID INT IDENTITY(1,1) NOT NULL, Name VARCHAR(55) NOT NULL DEFAULT 'mario', Type VARCHAR(55))");
        $this->pdo->exec("ALTER TABLE autoIncrement ADD CONSTRAINT PK_AUTOINC PRIMARY KEY (ID)");
        $this->pdo->exec("ALTER TABLE autoIncrement ADD CONSTRAINT CHK_ID_CONTSTRAINT CHECK (ID > 0 AND ID < 20)");
        $this->pdo->exec("INSERT INTO autoIncrement (Name, Type) VALUES ('mario', 'plumber')");
        $this->pdo->exec("INSERT INTO autoIncrement (Name, Type) VALUES ('luigi', 'plumber')");
        $this->pdo->exec("INSERT INTO autoIncrement (Name, Type) VALUES ('toad', 'mushroom')");
        $this->pdo->exec("INSERT INTO autoIncrement (Name, Type) VALUES ('princess', 'royalty')");
        $this->pdo->exec("INSERT INTO autoIncrement (Name, Type) VALUES ('wario', 'badguy')");
        $this->pdo->exec("INSERT INTO autoIncrement (Name, Type) VALUES ('yoshi', 'horse?')");
        // add unique key
        $this->pdo->exec("ALTER TABLE autoIncrement ADD CONSTRAINT UNI_KEY_1 UNIQUE (Name, Type)");
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
    protected function createTextTable(CsvFile $file, $primaryKey = null, $overrideTableName = null)
    {
        if (!$overrideTableName) {
            $tableName = $this->generateTableName($file);
        } else {
            $tableName = $overrideTableName;
        }

        $sql = sprintf(
            'CREATE TABLE %s (%s)',
            $tableName,
            implode(
                ', ',
                array_map(
                    function ($column) {
                        return $column . ' varchar(255) NULL';
                    },
                    $file->getHeader()
                )
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
                'ALTER TABLE %s ADD CONSTRAINT PK_%s PRIMARY KEY (%s)',
                $tableName,
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
                        array_map(
                            function ($data) {
                                if ($data == "") {
                                    return 'null';
                                }
                                if (is_numeric($data)) {
                                    return "'" . $data . "'";
                                }

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

                                $data = str_replace("'", "''", $data);

                                return "'" . $data . "'";
                            },
                            $file->current()
                        )
                    )
                );
                $file->next();

                $sql = sprintf(
                    'INSERT INTO %s VALUES %s',
                    $tableName,
                    substr($sqlInserts, 0, -1)
                );

                $this->pdo->exec($sql);
            }
        }

        $this->pdo->commit();

        $count = $this->pdo->query(sprintf('SELECT COUNT(*) AS itemsCount FROM %s', $tableName))->fetchColumn();
        $this->assertEquals($this->countTable($file), (int) $count);
    }

    /**
     * Count records in CSV (with headers)
     *
     * @param  CsvFile $file
     * @return int
     */
    protected function countTable(CsvFile $file)
    {
        $linesCount = 0;
        foreach ($file as $i => $line) {
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

    public function tableExists($tableName)
    {
        $res = $this->pdo->query(
            sprintf(
                "SELECT * FROM information_schema.tables WHERE TABLE_NAME = %s",
                $this->pdo->quote($tableName)
            )
        );
        return !($res->rowCount() === 0);
    }
}
