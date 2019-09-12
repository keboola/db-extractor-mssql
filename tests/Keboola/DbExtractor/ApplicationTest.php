<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class ApplicationTest extends AbstractMSSQLTest
{
    /** @var string */
    protected $rootPath = __DIR__ . '/../../..';

    private function replaceConfig(array $config, string $format): void
    {
        @unlink($this->dataDir . '/config.json');
        @unlink($this->dataDir . '/config.yml');
        if ($format === self::CONFIG_FORMAT_JSON) {
            file_put_contents($this->dataDir . '/config.json', json_encode($config));
        } else if ($format === self::CONFIG_FORMAT_YAML) {
            file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));
        } else {
            throw new UserException("Invalid config format type [{$format}]");
        }
    }

    public function testTestConnectionAction(): void
    {
        $config = $this->getConfig('mssql');
        $config['action'] = 'testConnection';

        $this->replaceConfig($config, self::CONFIG_FORMAT_YAML);
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
        $this->replaceConfig($config, self::CONFIG_FORMAT_YAML);

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
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'mssql',
            'remotePort' => '1433',
            'localPort' => '1234',
        ];
        $this->replaceConfig($config, self::CONFIG_FORMAT_YAML);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
        // verify that the bcp command uses the proxy
        $this->assertStringContainsString("-S \"127.0.0.1,1234\"", $process->getOutput());

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
        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

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
        $this->replaceConfig($config, self::CONFIG_FORMAT_YAML);

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

        $this->replaceConfig($config, self::CONFIG_FORMAT_YAML);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());

        $this->assertStringContainsString("[in.c-main.special]: DB query failed:", $process->getErrorOutput());

        $this->assertStringContainsString("The BCP export failed:", $process->getOutput());
        $this->assertStringContainsString("Attempting export using pdo", $process->getOutput());
    }

    public function testPdoFallback(): void
    {
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        $config['parameters']['tables'][0]['query'] = "SELECT *  FROM \"special\";";

        $this->replaceConfig($config, self::CONFIG_FORMAT_YAML);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $output = $process->getOutput() . "\n" . $process->getErrorOutput();

        $this->assertEquals(0, $process->getExitCode(), $output);
        $this->assertEquals('', $process->getErrorOutput());

        $this->assertStringContainsString("The BCP export failed:", $process->getOutput());
        $this->assertStringContainsString("Attempting export using pdo", $process->getOutput());
    }

    public function testDisableFallback(): void
    {
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        $config['parameters']['tables'][0]['query'] = "SELECT *  FROM \"special\";";
        $config['parameters']['tables'][0]['disableFallback'] = true;

        $this->replaceConfig($config, self::CONFIG_FORMAT_YAML);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertEquals("The BCP command produced an invalid csv.\n", $process->getErrorOutput());
    }

    public function testDisableBcp(): void
    {
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        $config['parameters']['tables'][0]['query'] = "SELECT *  FROM \"special\";";
        $config['parameters']['tables'][0]['disableBcp'] = true;

        $this->replaceConfig($config, self::CONFIG_FORMAT_YAML);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertStringContainsString(
            "The BCP export failed: BCP export was disabled by configuration. Attempting export using pdo_sqlsrv",
            $process->getOutput()
        );
    }

    public function testDisableBcpAndFallbackIsInvalidForTables(): void
    {
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        $config['parameters']['tables'][0]['query'] = "SELECT *  FROM \"special\";";
        $config['parameters']['tables'][0]['disableBcp'] = true;
        $config['parameters']['tables'][0]['disableFallback'] = true;

        $this->replaceConfig($config, self::CONFIG_FORMAT_YAML);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertStringContainsString(
            'Can\'t disable both BCP and fallback to PDO',
            $process->getErrorOutput()
        );
    }

    public function testDisableBcpAndFallbackIsInvalidForConfigRow(): void
    {
        $config = $this->getConfigRow('mssql');
        $config['parameters']['query'] = "SELECT *  FROM \"special\";";
        $config['parameters']['disableBcp'] = true;
        $config['parameters']['disableFallback'] = true;

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertStringContainsString(
            'Can\'t disable both BCP and fallback to PDO',
            $process->getErrorOutput()
        );
    }



    public function testDisableFallbackConfigRow(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['table']);
        $config['parameters']['query'] = "SELECT *  FROM \"special\";";
        $config['parameters']['disableFallback'] = true;

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertEquals("The BCP command produced an invalid csv.\n", $process->getErrorOutput());
    }

    public function testWhereClauseWithSingleQuotes(): void
    {
        $config = $this->getConfig('mssql');
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);
        unset($config['parameters']['tables'][3]);
        $config['parameters']['tables'][0]['query'] = "SELECT \"usergender\", \"sku\"  FROM \"sales\" WHERE \"usergender\" LIKE 'male'";

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $output = $process->getOutput() . "\n" . $process->getErrorOutput();

        $this->assertEquals(0, $process->getExitCode(), $output);
        $this->assertEquals('', $process->getErrorOutput());

        $this->assertStringContainsString("BCP successfully exported", $process->getOutput());
        $this->assertStringNotContainsString("The BCP export failed:", $process->getOutput());
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

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $dataFile = $this->dataDir . '/out/tables/in.c-main.simple_empty.csv';
        $manifestFile = $this->dataDir . '/out/tables/in.c-main.simple_empty.csv.manifest';
        @unlink($dataFile);
        @unlink($manifestFile);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertStringContainsString('[in.c-main.simple_empty]: Query returned empty result so nothing was imported', $process->getErrorOutput());

        $this->assertStringContainsString("[in.c-main.simple_empty]: The BCP export failed:", $process->getOutput());
        $this->assertStringContainsString("Attempting export using pdo", $process->getOutput());

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

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertStringContainsString("Executing \"SELECT [ID], [PROB_COL] FROM [dbo].[PDO_TEST]\" via PDO", $process->getOutput());

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

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertStringContainsString("SELECT [ID], [SMALLDATE] FROM [dbo].[SMALLDATETIME_TEST]", $process->getOutput());
        $this->assertStringNotContainsString("CONVERT(DATETIME2(0),[SMALLDATE])", $process->getOutput());
        $this->dropTable("SMALLDATETIME_TEST");
    }

    public function testIncrementalFetchingRun(): void
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

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $this->assertFileNotExists($this->dataDir . '/in/state.json');

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertFileExists($this->dataDir . '/out/state.json');
        $state = json_decode(file_get_contents($this->dataDir . "/out/state.json"), true);
        $this->assertEquals(["lastFetchedRow" => 6], $state);
    }

    public function testIncrementalFetchingWithDatetimeAndTimestampRun(): void
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
        $config['parameters']['incrementalFetchingColumn'] = 'datetime';

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertStringNotContainsString(
            "The BCP export failed: SQLSTATE[42000]: [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Implicit conversion from data type nvarchar to timestamp is not allowed.",
            $process->getOutput()
        );
        $this->assertFileExists($this->dataDir . '/out/state.json');
        $state = json_decode(file_get_contents($this->dataDir . "/out/state.json"), true);
        $this->assertLessThanOrEqual(2, time() - strtotime($state['lastFetchedRow']));
    }

    public function testIncrementalFetchingWithNullSmalldatetimeValues(): void
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
        $config['parameters']['incrementalFetchingColumn'] = 'smalldatetime';
        $config['parameters']['nolock'] = true;

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);
        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertStringNotContainsString(
            "The BCP export failed",
            $process->getOutput()
        );
        $outputFile = $this->dataDir . '/out/tables/in.c-main.auto-increment-timestamp.csv';
        $this->assertFileExists($this->dataDir . '/out/state.json');
        $state = json_decode(file_get_contents($this->dataDir . "/out/state.json"), true);
        $this->assertArrayHasKey('lastFetchedRow', $state);
        $this->assertEquals('2012-01-10 10:25:00', $state['lastFetchedRow']);
        unlink($outputFile);
        sleep(2);
        //now add a couple rows and run it again.
        $this->pdo->exec('INSERT INTO [auto Increment Timestamp] ([Weir%d Na-me], [smalldatetime]) VALUES (\'charles\', null), (\'william\', \'2012-01-10 10:55\')');

        if (!is_dir($this->dataDir . '/in')) {
            mkdir($this->dataDir . '/in');
            touch($this->dataDir . '/in/state.json');
        }
        // write state file
        file_put_contents($this->dataDir . '/in/state.json', json_encode($state));

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $this->assertStringNotContainsString(
            "The BCP export failed",
            $process->getOutput()
        );

        //check that output state contains expected information (will contain the same last 2 rows as above, + 2 more
        $state = json_decode(file_get_contents($this->dataDir . "/out/state.json"), true);
        $this->assertEquals('2012-01-10 10:55:00', $state['lastFetchedRow']);
    }

    public function testRunWithNoLock(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'auto Increment Timestamp',
            'schema' => 'dbo',
        ];
        $config['parameters']['nolock'] = true;

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
    }

    public function testDeprecatedConfigWithNoLock(): void
    {
        $config = $this->getConfig(self::DRIVER, self::CONFIG_FORMAT_JSON);
        $config['parameters']['tables'][1]['nolock'] = true;

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();
    }

    public function testRunAdvancedQueryWithNoLock(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['nolock'] = true;
        unset($config['parameters']['table']);
        $config['parameters']['query'] = "SELECT * FROM special";

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertStringNotContainsString(
            "nolock",
            $process->getOutput(),
            "Failed to assert that advanced query with nolock set didn't have nolock in the query"
        );
    }

    public function testTimestampHasUtf8Output(): void
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

        $this->replaceConfig($config, self::CONFIG_FORMAT_JSON);

        $process = new Process('php ' . $this->rootPath . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->mustRun();

        $output = iterator_to_array(new CsvFile($this->dataDir . '/out/tables/in.c-main.auto-increment-timestamp.csv'));

        foreach ($output as $line) {
            // assert the timestamp column is valid UTF-8
            $this->assertTrue(mb_check_encoding($output[7], 'UTF-8'));
        }
    }
}
