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
        $outputCsvFile = new CsvFile($this->dataDir . '/out/tables/in.c-main.sales.csv');
        $manifestFile = $this->dataDir . '/out/tables/in.c-main.sales.csv.manifest';
        @unlink($outputCsvFile);
        @unlink($manifestFile);

        // create the table in the DB
        if (!$this->tableExists("sales")) {
            $this->createTextTable(new CsvFile($this->dataDir . "/mssql/sales.csv"), ['createdat']);
        }

        $config = $this->getConfig('mssql');
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/src/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getErrorOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());

        $this->assertTrue($outputCsvFile->isFile());
        $this->assertFileExists($manifestFile);
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

        var_dump($process->getErrorOutput());
        var_dump($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
        $this->assertJson($process->getOutput());
    }
}