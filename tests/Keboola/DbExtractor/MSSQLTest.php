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
		$this->createTextTable($csv1, ['createdat']);

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
		$this->createTextTable($csv1, ['createdat']);


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
        $this->createTextTable($csv1, ['createdat']);

        $app = new Application($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(2, $result['tables']);
        $this->assertArrayHasKey('name', $result['tables'][0]);
        $this->assertEquals("sales", $result['tables'][0]['name']);
        $this->assertArrayHasKey('columns', $result['tables'][0]);
        $this->assertCount(12, $result['tables'][0]['columns']);
        $this->assertArrayHasKey('name', $result['tables'][0]['columns'][0]);
        $this->assertEquals("usergender", $result['tables'][0]['columns'][0]['name']);
        $this->assertArrayHasKey('type', $result['tables'][0]['columns'][0]);
        $this->assertEquals("varchar", $result['tables'][0]['columns'][0]['type']);
        $this->assertArrayHasKey('length', $result['tables'][0]['columns'][0]);
        $this->assertEquals(255, $result['tables'][0]['columns'][0]['length']);
        $this->assertArrayHasKey('nullable', $result['tables'][0]['columns'][0]);
        $this->assertTrue($result['tables'][0]['columns'][0]['nullable']);
        $this->assertArrayHasKey('default', $result['tables'][0]['columns'][0]);
        $this->assertNull($result['tables'][0]['columns'][0]['default']);
        $this->assertArrayHasKey('primaryKey', $result['tables'][0]['columns'][0]);
        $this->assertFalse($result['tables'][0]['columns'][0]['primaryKey']);

        // note the column fetch is ordered by ordinal_position so the assertion of column index must hold.
        // also, mssql ordinal_position is 1 based
        $this->assertArrayHasKey('ordinalPosition', $result['tables'][0]['columns'][6]);
        $this->assertEquals(7, $result['tables'][0]['columns'][6]['ordinalPosition']);

        // check that the primary key is set
        $this->assertEquals('createdat', $result['tables'][0]['columns'][5]['name']);
        $this->assertArrayHasKey('primaryKey', $result['tables'][0]['columns'][5]);
        // PK cannot be nullable
        $this->assertEquals(64, $result['tables'][0]['columns'][5]['length']);
        $this->assertFalse($result['tables'][0]['columns'][5]['nullable']);
        $this->assertTrue($result['tables'][0]['columns'][5]['primaryKey']);
    }
}
