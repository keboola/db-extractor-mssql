<?php
/**
 * @package ex-db-mssql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Yaml\Yaml;

class MSSQLTest extends AbstractMSSQLTest
{
	public function testCredentials()
	{
		$config = $this->getConfig('mssql');
		$config['action'] = 'testConnection';
		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('success', $result['status']);
	}

	public function testRunWithoutTables()
	{
		$config = $this->getConfig('mssql');

		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('success', $result['status']);
	}

	public function testRun()
	{
		$config = $this->getConfig('mssql');

		$app = $this->createApplication($config);

		$csv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');

		// set createdat as PK
		if (!$this->tableExists("sales")) {
            $this->createTextTable($csv1, ['createdat']);
        }

		$result = $app->run();

		$outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';

		$this->assertEquals('success', $result['status']);
		$this->assertFileExists($outputCsvFile);
		$this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
		$this->assertFileEquals((string) $csv1, $outputCsvFile);
	}

	public function testCredentialsWithSSH()
	{
		$config = $this->getConfig('mssql');
		$config['action'] = 'testConnection';

		$config['parameters']['db']['ssh'] = [
			'enabled' => true,
			'keys' => [
				'#private' => $this->getEnv('mssql', 'DB_SSH_KEY_PRIVATE'),
				'public' => $this->getEnv('mssql', 'DB_SSH_KEY_PUBLIC')
			],
			'user' => 'root',
			'sshHost' => 'sshproxy',
			'remoteHost' => 'mssql',
			'remotePort' => '1433',
			'localPort' => '1235',
		];

		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('success', $result['status']);
	}

	public function testRunWithSSH()
	{
		$config = $this->getConfig('mssql');
		$config['parameters']['db']['ssh'] = [
			'enabled' => true,
			'keys' => [
				'#private' => $this->getEnv('mssql', 'DB_SSH_KEY_PRIVATE'),
				'public' => $this->getEnv('mssql', 'DB_SSH_KEY_PUBLIC')
			],
			'user' => 'root',
			'sshHost' => 'sshproxy',
			'remoteHost' => 'mssql',
			'remotePort' => '1433',
			'localPort' => '1234',
		];

		$app = $this->createApplication($config);

		$csv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');

        if (!$this->tableExists("sales")) {
            $this->createTextTable($csv1, ['createdat']);
        }

		$result = $app->run();


		$outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';

		$this->assertEquals('success', $result['status']);
		$this->assertFileExists($outputCsvFile);
		$this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
		$this->assertFileEquals((string) $csv1, $outputCsvFile);
	}

    public function testGetTables()
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';

        $csv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');
        if (!$this->tableExists("sales")) {
            $this->createTextTable($csv1, ['createdat']);
        }
        if (!$this->tableExists("sales2")) {
            $this->createTextTable($csv1, null, "sales2");
        }

        // drop the t1 demo table if it exists
        $this->pdo->exec("DROP TABLE IF EXISTS 't1'");

        // set up a foreign key relationship
        $this->pdo->exec("ALTER TABLE sales2 ALTER COLUMN createdat varchar(64) NOT NULL");
        $this->pdo->exec("ALTER TABLE sales2 ADD CONSTRAINT FK_sales_sales2 FOREIGN KEY (createdat) REFERENCES sales(createdat)");

        $app = new Application($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(2, $result['tables']);
        foreach ($result['tables'] as $table) {
            $this->assertArrayHasKey('name', $table);
            $this->assertArrayHasKey('schema', $table);
            $this->assertArrayHasKey('type', $table);
            $this->assertArrayHasKey('rowCount', $table);
            $this->assertArrayHasKey('columns', $table);

            $this->assertEquals('test', $table['schema']);
            $this->assertEquals('BASE TABLE', $table['type']);
            $this->assertCount(12, $table['columns']);
            foreach ($table['columns'] as $i => $column) {
                $this->assertArrayHasKey('name', $column);
                $this->assertArrayHasKey('type', $column);
                $this->assertArrayHasKey('length', $column);
                $this->assertArrayHasKey('default', $column);
                $this->assertArrayHasKey('nullable', $column);
                $this->assertArrayHasKey('primaryKey', $column);
                $this->assertArrayHasKey('foreignKey', $column);
                $this->assertArrayHasKey('uniqueKey', $column);
                $this->assertArrayHasKey('ordinalPosition', $column);
                // values
                $this->assertEquals("text", $column['type']);
                $this->assertEquals($i + 1, $column['ordinalPosition']);
                if ($column['name'] === 'createdat') {
                    $this->assertArrayHasKey('constraintName', $column);
                    $this->assertEquals(60, $column['length']);
                    $this->assertFalse($column['nullable']);
                    if ($table['name'] === 'sales') {
                        $this->assertTrue($column['primaryKey']);
                        $this->assertFalse($column['foreignKey']);
                        $this->assertFalse($column['uniqueKey']);
                    } else {
                        $this->assertFalse($column['primaryKey']);
                        $this->assertTrue($column['foreignKey']);
                        $this->assertFalse($column['uniqueKey']);
                        $this->assertArrayHasKey('foreignKeyRefSchema', $column);
                        $this->assertArrayHasKey('foreignKeyRefSchema', $column);
                        $this->assertArrayHasKey('foreignKeyRefSchema', $column);
                        $this->assertEquals($column['foreignKeyRefSchema'], "dbo");
                        $this->assertEquals($column['foreignKeyRefTable'], "sales");
                        $this->assertEquals($column['foreignKeyRefColumn'], "createdat");
                    }
                } else {
                    $this->assertEquals(65535, $column['length']);
                    $this->assertTrue($column['nullable']);
                    $this->assertNull($column['default']);
                    $this->assertFalse($column['primaryKey']);
                    $this->assertFalse($column['foreignKey']);
                    $this->assertFalse($column['uniqueKey']);
                }
            }
        }
    }
}
