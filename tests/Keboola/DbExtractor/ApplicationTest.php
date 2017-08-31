<?php

namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class ApplicationTest extends AbstractMSSQLTest
{
    public function testTestConnectionAction()
    {
        $config = $this->getConfig('mssql');
        $config['action'] = 'testConnection';
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
        $this->assertJson($process->getOutput());
    }

    public function testRunAction()
    {
        $outputCsvFile1 = $this->dataDir . '/out/tables/in.c-main.sales.csv';
        $outputCsvFile2 = $this->dataDir . '/out/tables/in.c-main.tableColumns.csv';
        $manifestFile1 = $this->dataDir . '/out/tables/in.c-main.sales.csv.manifest';
        $manifestFile2 = $this->dataDir . '/out/tables/in.c-main.tableColumns.csv.manifest';
        @unlink($outputCsvFile1);
        @unlink($outputCsvFile2);
        @unlink($manifestFile1);
        @unlink($manifestFile2);

        $expectedCsv1 = new CsvFile($this->dataDir . '/mssql/sales.csv');
        $expectedCsv2 = new CsvFile($this->dataDir . '/mssql/tableColumns.csv');

        $config = $this->getConfig('mssql');
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());

        $this->assertFileExists($outputCsvFile1);
        var_dump($expectedCsv1);
        var_dump((string) $expectedCsv1);
        $this->assertFileEquals((string) $expectedCsv1, $outputCsvFile1);
        $this->assertFileExists($outputCsvFile2);
        $this->assertFileEquals((string) $expectedCsv2, $outputCsvFile2);
        $this->assertFileExists($manifestFile1);
        $this->assertFileExists($manifestFile2);
    }

    public function testGetTablesAction()
    {
        $config = $this->getConfig('mssql');
        $config['action'] = 'getTables';
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
        $this->assertJson($process->getOutput());
    }
}