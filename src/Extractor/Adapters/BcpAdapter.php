<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor\Adapters;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Throwable;
use Psr\Log\LoggerInterface;
use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\BcpAdapterException;
use Keboola\DbExtractor\Extractor\MetadataProvider;
use Keboola\DbExtractor\Extractor\MssqlDataType;
use Keboola\DbExtractor\Extractor\PdoConnection;
use Symfony\Component\Process\Process;

class BcpAdapter
{
    private LoggerInterface $logger;

    private PdoConnection $pdo;

    private MetadataProvider $metadataProvider;

    private DatabaseConfig $databaseConfig;

    private array $state;

    public function __construct(
        LoggerInterface $logger,
        PdoConnection $pdo,
        MetadataProvider $metadataProvider,
        DatabaseConfig $databaseConfig,
        array $state
    ) {
        $this->logger = $logger;
        $this->pdo = $pdo;
        $this->metadataProvider = $metadataProvider;
        $this->databaseConfig = $databaseConfig;
        $this->state = $state;
    }

    public function export(
        MssqlExportConfig $exportConfig,
        ?string $maxValue,
        string $query,
        string $csvPath,
        ?string $incrementalFetchingType
    ): array {
        touch($csvPath);
        $result = $this->runBcpCommand($query, $csvPath);

        if ($result['rows'] > 0) {
            if ($exportConfig->hasQuery()) {
                // Output CSV file is generated without header when using BCP,
                // so "columns" must be part of manifest files
                $result['bcpColumns'] = $this->getAdvancedQueryColumns($query, $exportConfig);
                $this->stripNullBytesInEmptyFields($csvPath);
            } else if ($exportConfig->isIncrementalFetching()) {
                if ($maxValue) {
                    $result['lastFetchedRow'] = $maxValue;
                } else if ($incrementalFetchingType === MssqlDataType::INCREMENT_TYPE_DATETIME) {
                    $result['lastFetchedRow'] = $this->getLastDatetimeValue($exportConfig, $result['lastFetchedRow']);
                } else {
                    $result['lastFetchedRow'] = $this->getLastValue($exportConfig, $result['lastFetchedRow']);
                }
            }
        }

        return $result;
    }

    private function stripNullBytesInEmptyFields(string $fileName): void
    {
        // this will replace null byte column values in the file
        // this is here because BCP will output null bytes for empty strings
        // this can occur in advanced queries where the column isn't sanitized
        $nullAtStart = 's/^\x00,/,/g';
        $nullAtEnd = 's/,\x00$/,/g';
        $nullInTheMiddle = 's/,\x00,/,,/g';
        $sedCommand = sprintf('sed -e \'%s;%s;%s\' -i %s', $nullAtStart, $nullInTheMiddle, $nullAtEnd, $fileName);

        $process = Process::fromShellCommandline($sedCommand);
        $process->setTimeout(1800);
        $process->run();
        if ($process->getExitCode() !== 0 || !empty($process->getErrorOutput())) {
            throw new ApplicationException(
                sprintf('Error Stripping Nulls: %s', $process->getErrorOutput())
            );
        }
    }

    private function getLastDatetimeValue(
        ExportConfig $exportConfig,
        array $lastRow
    ): string {
        $whereClause = '';
        $whereValues = [];

        $tableColumns =  $this->metadataProvider->getTable($exportConfig->getTable())->getColumns();
        $columnNames = $exportConfig->hasColumns() ?
            $exportConfig->getColumns() :
            $tableColumns->getNames();

        foreach ($columnNames as $key => $name) {
            $column = $tableColumns->getByName($name);
            if (strtoupper($column->getType()) === 'TIMESTAMP') {
                continue;
            }
            if ($whereClause !== '') {
                $whereClause .= ' AND ';
            }
            if (in_array(strtoupper($column->getType()), ['DATETIME", "DATETIME2'])) {
                $whereClause .=
                    'CONVERT(DATETIME2(0), ' . $this->pdo->quoteIdentifier($column->getName()) . ') = ?';
            } else {
                $whereClause .= $this->pdo->quoteIdentifier($column->getName()) . ' = ?';
            }
            $whereValues[] = $lastRow[$key];
        }

        $query = sprintf(
            'SELECT %s FROM %s.%s WHERE %s;',
            $this->pdo->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->pdo->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->pdo->quoteIdentifier($exportConfig->getTable()->getName()),
            $whereClause
        );

        $result = $this->pdo->runRetryableQuery($query, $exportConfig->getMaxRetries(), $whereValues);

        if (count($result) > 0) {
            return $result[0][$exportConfig->getIncrementalFetchingColumn()];
        }

        throw new ApplicationException('Fetching last datetime value returned no results');
    }

    private function getLastValue(ExportConfig $exportConfig, array $lastRow): string
    {
        $columnNames = $exportConfig->hasColumns() ?
            $exportConfig->getColumns() :
            $this->metadataProvider
            ->getTable($exportConfig->getTable())
            ->getColumns()
            ->getNames();

        foreach ($columnNames as $key => $name) {
            if ($name === $exportConfig->getIncrementalFetchingColumn()) {
                return $lastRow[$key];
            }
        }

        throw new ApplicationException('Fetching last id value returned no results');
    }

    public function getAdvancedQueryColumns(string $query, ExportConfig $exportConfig): ?array
    {
        // This will only work if the server is >= sql server 2012
        $sql = sprintf(
            "EXEC sp_describe_first_result_set N'%s', null, 0;",
            rtrim(trim(str_replace("'", "''", $query)), ';')
        );
        try {
            $result = $this->pdo->runRetryableQuery($sql, $exportConfig->getMaxRetries());
            if (is_array($result) && !empty($result)) {
                return array_map(
                    function ($row) {
                        return $row['name'];
                    },
                    $result
                );
            }
            return null;
        } catch (Throwable $e) {
            throw new BcpAdapterException(
                sprintf('DB query "%s" failed: %s', $sql, $e->getMessage()),
                0,
                $e
            );
        }
    }

    private function runBcpCommand(string $query, string $filename): array
    {
        $process = Process::fromShellCommandline($this->createBcpCommand($filename, $query));
        $process->setTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            throw new BcpAdapterException(sprintf(
                "Export process failed. Output: %s. \n\n Error Output: %s.",
                $process->getOutput(),
                $process->getErrorOutput()
            ));
        }

        try {
            $output = $this->processOutputCsv($filename, $process);
        } catch (BcpAdapterException $e) {
            throw $e;
        } catch (\Throwable $e) {
            throw new BcpAdapterException(
                'The BCP command produced an invalid csv: ' . $e->getMessage(),
                0,
                $e
            );
        }

        $this->logger->info(sprintf('BCP successfully exported %d rows.', $output['rows']));
        return $output;
    }

    private function processOutputCsv(string $filename, Process $process): array
    {
        $outputFile = new CsvReader($filename);
        $numRows = 0;
        $lastFetchedRow = null;
        $colCount = $outputFile->getColumnsCount();
        while ($outputFile->valid()) {
            if (count($outputFile->current()) !== $colCount) {
                $lineNumber = $numRows + 1;

                throw new BcpAdapterException('The BCP command produced an invalid csv.', 0, null, [
                    'currentLineNumber' => $lineNumber,
                    'currentLine' => $outputFile->current(),
                    'bcpErrorOutput' => $process->getErrorOutput(),
                ]);
            }
            $lastRow = $outputFile->current();
            $outputFile->next();
            if (!$outputFile->valid()) {
                $lastFetchedRow = $lastRow;
            }
            $numRows++;
        }

        $output = ['rows' => $numRows];
        if ($lastFetchedRow) {
            $output['lastFetchedRow'] = $lastFetchedRow;
        }
        return $output;
    }

    private function createBcpCommand(string $filename, string $query): string
    {
        $serverName = $this->databaseConfig->getHost();
        $serverName .= $this->databaseConfig->hasPort() ? ',' . $this->databaseConfig->getPort() : '';

        $cmd = sprintf(
            'bcp %s queryout %s -S %s -U %s -P %s -d %s -q -k -b 50000 -m 1 -t "," -r "\n" -c',
            escapeshellarg($query),
            escapeshellarg($filename),
            escapeshellarg($serverName),
            escapeshellarg($this->databaseConfig->getUsername()),
            escapeshellarg($this->databaseConfig->getPassword()),
            escapeshellarg($this->databaseConfig->getDatabase())
        );

        $this->logger->info(sprintf(
            'Executing BCP command: %s',
            preg_replace('/\-P.*\-d/', '-P ***** -d', $cmd)
        ));
        return $cmd;
    }
}
