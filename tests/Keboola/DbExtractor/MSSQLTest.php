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
            $this->assertArrayHasKey('columns', $table);

            $this->assertEquals('test', $table['catalog']);
            $this->assertEquals('dbo', $table['schema']);
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
                $this->assertEquals("varchar", $column['type']);
                $this->assertEquals($i + 1, $column['ordinalPosition']);
                if ($column['name'] === 'createdat') {
                    $this->assertArrayHasKey('constraintName', $column);
                    $this->assertEquals(64, $column['length']);
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
                    $this->assertEquals(255, $column['length']);
                    $this->assertTrue($column['nullable']);
                    $this->assertNull($column['default']);
                    $this->assertFalse($column['primaryKey']);
                    $this->assertFalse($column['foreignKey']);
                    $this->assertFalse($column['uniqueKey']);
                }
            }
        }
    }

    public function testManifestMetadata()
    {
        $config = $this->getConfig();

        $config['parameters']['tables'][0]['columns'] = ["usergender","usercity","usersentiment","zipcode", "createdat"];
        $config['parameters']['tables'][0]['table'] = 'sales';
        $config['parameters']['tables'][0]['query'] = "SELECT usergender, usercity, usersentiment, zipcode FROM sales";
        // use just 1 table
        unset($config['parameters']['tables'][1]);

        $app = new Application($config);

        $result = $app->run();

        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest')
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);
        foreach ($outputManifest['metadata'] as $i => $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            switch ($metadata['key']) {
                case 'KBC.name':
                    $this->assertEquals('sales', $metadata['value']);
                    break;
                case 'KBC.catalog':
                    $this->assertEquals('test', $metadata['value']);
                    break;
                case 'KBC.schema':
                    $this->assertEquals('dbo', $metadata['value']);
                    break;
                case 'KBC.type':
                    $this->assertEquals('BASE TABLE', $metadata['value']);
                    break;
                default:
                    $this->fail('Unknown table metadata key: ' . $metadata['key']);
            }
        }
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(5, $outputManifest['column_metadata']);
        foreach ($outputManifest['column_metadata']['createdat'] as $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            switch ($metadata['key']) {
                case 'KBC.datatype.type':
                    $this->assertEquals('varchar', $metadata['value']);
                    break;
                case 'KBC.datatype.basetype':
                    $this->assertEquals('STRING', $metadata['value']);
                    break;
                case 'KBC.datatype.nullable':
                    $this->assertFalse($metadata['value']);
                    break;
                case 'KBC.datatype.default':
                    $this->assertNull($metadata['value']);
                    break;
                case 'KBC.datatype.length':
                    $this->assertEquals('64', $metadata['value']);
                    break;
                case 'KBC.primaryKey':
                    $this->assertTrue($metadata['value']);
                    break;
                case 'KBC.ordinalPosition':
                    $this->assertGreaterThan(1, $metadata['value']);
                    break;
                case 'KBC.uniqueKey':
                    $this->assertFalse($metadata['value']);
                    break;
                default:
                    break;
            }
        }
    }
}
