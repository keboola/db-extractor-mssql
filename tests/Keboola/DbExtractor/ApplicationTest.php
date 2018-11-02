<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class ApplicationTest extends AbstractMSSQLTest
{
    /** @var string */
    protected $rootPath = __DIR__ . '/../../..';
    
    public function testTestConnectionAction(): void
    {
        $config = $this->getConfig('mssql');
        $config['action'] = 'testConnection';
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
        $this->assertJson($process->getOutput());
    }

    public function testRunAction(): void
    {
        $outputCsvFile1 = $this->dataDir . '/out/tables/in.c-main.sales.csv';
        $outputCsvFile2 = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv';
        $outputCsvFile4 = $this->dataDir . '/out/tables/in.c-main.special.csv';
        $manifestFile1 = $this->dataDir . '/out/tables/in.c-main.sales.csv.manifest';
        $manifestFile2 = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest';
        $manifestFile4 = $this->dataDir . '/out/tables/in.c-main.special.csv.manifest';

        @unlink($outputCsvFile1);
        @unlink($outputCsvFile2);
        @unlink($outputCsvFile4);
        @unlink($manifestFile1);
        @unlink($manifestFile2);
        @unlink($manifestFile4);

        $expectedCsv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');
        $expectedCsv1 = iterator_to_array($expectedCsv1);

        $expectedCsv2 = new CsvFile($this->dataDir . '/mssql/tableColumns.csv');
        $expectedCsv2 = iterator_to_array($expectedCsv2);
        array_shift($expectedCsv2);
        $expectedCsv4 = new CsvFile($this->dataDir . '/mssql/special.csv');
        $expectedCsv4 = iterator_to_array($expectedCsv4);
        array_shift($expectedCsv4);

        $config = $this->getConfig('mssql');
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());

        $outputCsvData1 = iterator_to_array(new CsvFile($outputCsvFile1));
        $outputCsvData2 = iterator_to_array(new CsvFile($outputCsvFile2));
        $outputCsvData4 = iterator_to_array(new CsvFile($outputCsvFile4));

        $this->assertFileExists($outputCsvFile1);
        $this->assertEquals(ksort($expectedCsv1), ksort($outputCsvData1));
        $this->assertFileExists($outputCsvFile2);
        $this->assertEquals(ksort($expectedCsv2), ksort($outputCsvData2));
        $this->assertFileExists($outputCsvFile4);
        $this->assertEquals(ksort($expectedCsv4), ksort($outputCsvData4));
        $this->assertFileExists($manifestFile1);
        $this->assertFileExists($manifestFile2);
        $this->assertFileExists($manifestFile4);
    }

    public function testRunActionSshTunnel(): void
    {
        $outputCsvFile1 = $this->dataDir . '/out/tables/in.c-main.sales.csv';
        $outputCsvFile2 = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv';
        $outputCsvFile4 = $this->dataDir . '/out/tables/in.c-main.special.csv';
        $manifestFile1 = $this->dataDir . '/out/tables/in.c-main.sales.csv.manifest';
        $manifestFile2 = $this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.manifest';
        $manifestFile4 = $this->dataDir . '/out/tables/in.c-main.special.csv.manifest';

        @unlink($outputCsvFile1);
        @unlink($outputCsvFile2);
        @unlink($outputCsvFile4);
        @unlink($manifestFile1);
        @unlink($manifestFile2);
        @unlink($manifestFile4);

        $expectedCsv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');
        $expectedCsv1 = iterator_to_array($expectedCsv1);

        $expectedCsv2 = new CsvFile($this->dataDir . '/mssql/tableColumns.csv');
        $expectedCsv2 = iterator_to_array($expectedCsv2);
        array_shift($expectedCsv2);
        $expectedCsv4 = new CsvFile($this->dataDir . '/mssql/special.csv');
        $expectedCsv4 = iterator_to_array($expectedCsv4);
        array_shift($expectedCsv4);

        $config = $this->getConfig('mssql');
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey('mssql'),
                'public' => $this->getEnv('mssql', 'DB_SSH_KEY_PUBLIC'),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'mssql',
            'remotePort' => '1433',
            'localPort' => '1234',
        ];
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
        // verify that the bcp command uses the proxy
        $this->assertContains("-S \"127.0.0.1,1234\"", $process->getOutput());

        $outputCsvData1 = iterator_to_array(new CsvFile($outputCsvFile1));
        $outputCsvData2 = iterator_to_array(new CsvFile($outputCsvFile2));
        $outputCsvData4 = iterator_to_array(new CsvFile($outputCsvFile4));

        $this->assertFileExists($outputCsvFile1);
        $this->assertEquals(ksort($expectedCsv1), ksort($outputCsvData1));
        $this->assertFileExists($outputCsvFile2);
        $this->assertEquals(ksort($expectedCsv2), ksort($outputCsvData2));
        $this->assertFileExists($outputCsvFile4);
        $this->assertEquals(ksort($expectedCsv4), ksort($outputCsvData4));
        $this->assertFileExists($manifestFile1);
        $this->assertFileExists($manifestFile2);
        $this->assertFileExists($manifestFile4);
    }

    public function testRunActionJsonConfig(): void
    {
        $config = $this->getConfig('mssql', 'json');
        @unlink($this->dataDir . '/config.yml');
        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testGetTablesAction(): void
    {
        $config = $this->getConfig('mssql');
        $config['action'] = 'getTables';
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
        $this->assertJson($process->getOutput());
    }

    public function testRunError(): void
    {
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]['table']);
        $config['parameters']['tables'][3]['query'] = "SELECT SOMETHING INVALID FROM \"dbo\".\"special\"";
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());

        var_dump($process->getErrorOutput());
        $this->assertContains("[in.c-main.special]: DB query failed:", $process->getErrorOutput());

        $this->assertContains("The BCP export failed:", $process->getOutput());
        $this->assertContains("Attempting export using pdo", $process->getOutput());
    }

    public function testPdoFallback(): void
    {
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        $config['parameters']['tables'][0]['query'] = "SELECT *  FROM \"special\";";

        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $output = $process->getOutput() . "\n" . $process->getErrorOutput();

        $this->assertEquals(0, $process->getExitCode(), $output);
        $this->assertEquals('', $process->getErrorOutput());

        $this->assertContains("The BCP export failed:", $process->getOutput());
        $this->assertContains("Attempting export using pdo", $process->getOutput());
    }

    public function testWhereClauseWithSingleQuotes(): void
    {
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        $config['parameters']['tables'][0]['query'] = "SELECT \"usergender\", \"sku\"  FROM \"sales\" WHERE \"usergender\" LIKE 'male'";
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $output = $process->getOutput() . "\n" . $process->getErrorOutput();

        $this->assertEquals(0, $process->getExitCode(), $output);
        $this->assertEquals('', $process->getErrorOutput());

        $this->assertContains("BCP successfully exported", $process->getOutput());
        $this->assertNotContains("The BCP export failed:", $process->getOutput());
    }

    public function testPDOFallbackSimpleNoData(): void
    {
        $this->pdo->exec("CREATE TABLE [Empty Test] ([wierd C\$name] varchar, col2 varchar);");
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        unset($config['parameters']['tables'][0]['query']);
        $config['parameters']['tables'][0]['name'] = "simple_empty";
        $config['parameters']['tables'][0]['outputTable'] = "in.c-main.simple_empty";
        $config['parameters']['tables'][0]['table'] = [
            "tableName" => "empty test",
            "schema" => "dbo",
        ];

        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $dataFile = $this->dataDir . '/out/tables/in.c-main.simple_empty.csv';
        $manifestFile = $this->dataDir . '/out/tables/in.c-main.simple_empty.csv.manifest';
        @unlink($dataFile);
        @unlink($manifestFile);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertContains('[in.c-main.simple_empty]: Query returned empty result so nothing was imported', $process->getErrorOutput());

        $this->assertContains("[in.c-main.simple_empty]: The BCP export failed:", $process->getOutput());
        $this->assertContains("Attempting export using pdo", $process->getOutput());

        $this->assertFileNotExists($dataFile);
        $this->assertFileNotExists($manifestFile);
    }

    public function testSimplifiedPdoFallbackQuery(): void
    {
        $this->dropTable("PDO_TEST");
        $this->pdo->exec("CREATE TABLE [PDO_TEST] ([ID] INT NULL, [PROB_COL] sql_variant DEFAULT null);");
        $this->pdo->exec(
            "INSERT INTO [PDO_TEST] VALUES 
            ('', GETDATE()), 
            ('', null)"
        );
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        unset($config['parameters']['tables'][0]['query']);
        $config['parameters']['tables'][0]['name'] = "pdo test";
        $config['parameters']['tables'][0]['table'] = ["tableName" => "PDO_TEST", "schema" => "dbo"];
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.pdo_test';

        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertContains("Executing \"SELECT * FROM [dbo].[PDO_TEST]\" via PDO", $process->getOutput());

        $this->dropTable("PDO_TEST");
    }

    public function testSmallDateTime(): void
    {
        $this->dropTable("SMALLDATETIME_TEST");
        $this->pdo->exec("CREATE TABLE [SMALLDATETIME_TEST] ([ID] INT NULL, [SMALLDATE] SMALLDATETIME NOT NULL);");
        $this->pdo->exec(
            "INSERT INTO [SMALLDATETIME_TEST] VALUES 
            (1, GETDATE()),
            (2, GETDATE())"
        );
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        unset($config['parameters']['tables'][0]['query']);
        $config['parameters']['tables'][0]['name'] = "smalldatetime";
        $config['parameters']['tables'][0]['table'] = ["tableName" => "SMALLDATETIME_TEST", "schema" => "dbo"];
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.smalldatetime_test';

        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertContains("SELECT [ID], [SMALLDATE] FROM [dbo].[SMALLDATETIME_TEST]", $process->getOutput());
        $this->assertNotContains("CONVERT(DATETIME2(0),[SMALLDATE])", $process->getOutput());
        $this->dropTable("SMALLDATETIME_TEST");
    }

    public function testIncrementalFetchingRun():void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'auto Increment Timestamp',
            'schema' => 'dbo',
        ];
        $config['parameters']['incremental'] = true;
        $config['parameters']['name'] = 'auto-increment-timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['primaryKey'] = ['_Weir%d I-D'];
        $config['parameters']['incrementalFetchingColumn'] = '_Weir%d I-D';

        @unlink($this->dataDir . '/config.json');
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertFileExists($this->dataDir . '/out/state.json');
        $state = json_decode(file_get_contents($this->dataDir . "/out/state.json"), true);
        $this->assertEquals(["lastFetchedRow" => 6], $state);
    }
}
