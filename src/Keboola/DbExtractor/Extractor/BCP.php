<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Symfony\Component\Process\Process;

class BCP
{
    /** @var array */
    private $dbParams;

    /** @var Logger */
    private $logger;

    /** @var string */
    private $errorFile = './tmp/ex-db-mssql-errors';

    public function __construct(array $dbParams, Logger $logger)
    {
        $this->dbParams = $dbParams;
        $this->logger = $logger;
        @unlink($this->errorFile);
    }

    public function export(string $query, string $filename): int
    {
        $process = new Process($this->createBcpCommand($filename, $query));
        $process->setTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            $errors = '';
            if (file_exists($this->errorFile)) {
                echo "\n THERE WAS A GODDAMNED ERROR\n";
                var_dump(file_get_contents($this->errorFile));
                $errors = file_get_contents($this->errorFile);
            }

            throw new UserException(sprintf(
                "Export process failed. Output: %s. \n\n Error Output: %s. \n\n Errors: %s",
                $process->getOutput(),
                $process->getErrorOutput(),
                $errors
            ));
        }
        $numRows = $this->countRows(new CsvFile($filename));

        return $numRows;
    }

    private function createBcpCommand(string $filename, string $query): string
    {
        $serverName = $this->dbParams['host'];
        $serverName .= !empty($this->dbParams['instance']) ? '\\' . $this->dbParams['instance'] : '';
        $serverName .= "," . $this->dbParams['port'];

        $cmd = sprintf(
            'bcp "%s" queryout %s -S "%s" -U %s -P "%s" -d %s -k -b50000 -e"%s" -m1 -t, -r"\n" -c',
            $query,
            $filename,
            $serverName,
            $this->dbParams['user'],
            $this->dbParams['#password'],
            $this->dbParams['database'],
            $this->errorFile
        );

        $this->logger->info(sprintf(
            "The export BCP command: %s",
            preg_replace('/\-P.".*".\-d/', '-P "*****" -d', $cmd)
        ));

        return $cmd;
    }

    protected function countRows(CsvFile $file): int
    {
        $linesCount = 0;
        foreach ($file as $i => $line) {
            $linesCount++;
        }
        return $linesCount;
    }
}