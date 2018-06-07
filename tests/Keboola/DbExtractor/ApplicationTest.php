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
        $this->assertContains("[special]: DB query failed:", $process->getErrorOutput());

        $this->assertContains("BCP command failed:", $process->getOutput());
        $this->assertContains("Attempting export using pdo", $process->getOutput());
    }

    public function testPdoFallback(): void
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

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());

        $this->assertContains("BCP command failed:", $process->getOutput());
        $this->assertContains("Attempting export using pdo", $process->getOutput());
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
        $this->assertContains('Nothing was imported for table [simple_empty]', $process->getErrorOutput());

        $this->assertContains("BCP command failed:", $process->getOutput());
        $this->assertContains("Attempting export using pdo", $process->getOutput());

        $this->assertFileNotExists($dataFile);
        $this->assertFileNotExists($manifestFile);
    }
}
