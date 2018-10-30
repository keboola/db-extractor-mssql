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

    /** @var bool */
    private $isIncrementalFetching;

    public function __construct(array $dbParams, Logger $logger, bool $isIncrementalFetching = false)
    {
        $this->dbParams = $dbParams;
        $this->logger = $logger;
        if ($isIncrementalFetching) {
            $this->isIncrementalFetching = $isIncrementalFetching;
        }
    }

    public function export(string $query, string $filename): array
    {
        $process = new Process($this->createBcpCommand($filename, $query));
        $process->setTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            throw new UserException(sprintf(
                "Export process failed. Output: %s. \n\n Error Output: %s.",
                $process->getOutput(),
                $process->getErrorOutput()
            ));
        }

        $outputFile = new CsvFile($filename);
        $numRows = 0;
        $lastFetchedRow = null;
        $colCount = $outputFile->getColumnsCount();
        while ($outputFile->valid()) {
            if (count($outputFile->current()) !== $colCount) {
                throw new UserException("The BCP command produced an invalid csv.");
            }
            $lastRow = $outputFile->current();
            $outputFile->next();
            if ($this->isIncrementalFetching && !$outputFile->valid()) {
                $lastFetchedRow = $lastRow;
            }
            $numRows++;
        }
        $this->logger->info(sprintf("BCP successfully exported %d rows.", $numRows));
        $output = ['rows' => $numRows];
        if ($lastFetchedRow) {
            $output['lastFetchedRow'] = $lastFetchedRow;
        }
        return $output;
    }

    private function createBcpCommand(string $filename, string $query): string
    {
        $serverName = $this->dbParams['host'];
        $serverName .= !empty($this->dbParams['instance']) ? '\\' . $this->dbParams['instance'] : '';
        $serverName .= "," . $this->dbParams['port'];

        $cmd = sprintf(
            'bcp "%s" queryout %s -S "%s" -U %s -P "%s" -d %s -k -b50000 -m1 -t, -r"\n" -c',
            $query,
            $filename,
            $serverName,
            $this->dbParams['user'],
            $this->dbParams['#password'],
            $this->dbParams['database']
        );

        $this->logger->info(sprintf(
            "Executing this BCP command: %s",
            preg_replace('/\-P.".*".\-d/', '-P "*****" -d', $cmd)
        ));

        return $cmd;
    }
}
