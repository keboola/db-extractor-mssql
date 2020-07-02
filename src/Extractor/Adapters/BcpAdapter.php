<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor\Adapters;

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

    private array $dbParams;

    private array $state;

    public function __construct(
        LoggerInterface $logger,
        PdoConnection $pdo,
        MetadataProvider $metadataProvider,
        array $dbParams,
        array $state
    ) {
        $this->logger = $logger;
        $this->pdo = $pdo;
        $this->metadataProvider = $metadataProvider;
        $this->dbParams = $dbParams;
        $this->state = $state;
    }

    public function export(
        array $table,
        ?string $maxValue,
        string $query,
        ?array $incrementalFetching,
        string $csvPath
    ): array {
        $isAdvancedQuery = array_key_exists('query', $table);
        touch($csvPath);
        $result = $this->runBcpCommand($query, $csvPath);

        if ($result['rows'] > 0) {
            if ($isAdvancedQuery) {
                // Output CSV file is generated without header when using BCP,
                // so "columns" must be part of manifest files
                $result['bcpColumns'] = $this->getAdvancedQueryColumns($query);
                $this->stripNullBytesInEmptyFields($csvPath);
            } else if ($incrementalFetching && isset($incrementalFetching['column'])) {
                if ($maxValue) {
                    $result['lastFetchedRow'] = $maxValue;
                } else if ($incrementalFetching['type'] === MssqlDataType::INCREMENT_TYPE_DATETIME) {
                    $result['lastFetchedRow'] = $this->getLastFetchedDatetimeValue(
                        $result['lastFetchedRow'],
                        $table['table'],
                        $incrementalFetching,
                        $this->metadataProvider->getColumnsMetadata($table)
                    );
                } else {
                    $result['lastFetchedRow'] = $this->getLastFetchedId(
                        $incrementalFetching,
                        $this->metadataProvider->getColumnsMetadata($table),
                        $result['lastFetchedRow']
                    );
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

    private function getLastFetchedDatetimeValue(
        array $lastExportedLine,
        array $table,
        array $incrementalFetching,
        array $columnMetadata
    ): string {
        $whereClause = '';
        $whereValues = [];

        foreach ($columnMetadata as $key => $column) {
            if (strtoupper($column['type']) === 'TIMESTAMP') {
                continue;
            }
            if ($whereClause !== '') {
                $whereClause .= ' AND ';
            }
            if (in_array(strtoupper($column['type']), ['DATETIME", "DATETIME2'])) {
                $whereClause .=
                    'CONVERT(DATETIME2(0), ' . $this->pdo->quoteIdentifier($column['name']) . ') = ?';
            } else {
                $whereClause .= $this->pdo->quoteIdentifier($column['name']) . ' = ?';
            }
            $whereValues[] = $lastExportedLine[$key];
        }

        $query = sprintf(
            'SELECT %s FROM %s.%s WHERE %s;',
            $this->pdo->quoteIdentifier($incrementalFetching['column']),
            $this->pdo->quoteIdentifier($table['schema']),
            $this->pdo->quoteIdentifier($table['tableName']),
            $whereClause
        );

        $maxTries = isset($table['retries']) ? (int) $table['retries'] : Extractor::DEFAULT_MAX_TRIES;
        $result = $this->pdo->runRetryableQuery($query, $maxTries, $whereValues);
        if (count($result) > 0) {
            return $result[0][$incrementalFetching['column']];
        }

        throw new ApplicationException('Fetching last datetime value returned no results');
    }

    private function getLastFetchedId(
        array $incrementalFetching,
        array $columnMetadata,
        array $lastExportedLine
    ): string {
        $incrementalFetchingColumnIndex = null;
        foreach ($columnMetadata as $key => $column) {
            if ($column['name'] === $incrementalFetching['column']) {
                return $lastExportedLine[$key];
            }
        }

        throw new ApplicationException('Fetching last id value returned no results');
    }

    public function getAdvancedQueryColumns(string $query): ?array
    {
        // This will only work if the server is >= sql server 2012
        $sql = sprintf(
            "EXEC sp_describe_first_result_set N'%s', null, 0;",
            rtrim(trim(str_replace("'", "''", $query)), ';')
        );
        try {
            $result = $this->pdo->runRetryableQuery($sql, Extractor::DEFAULT_MAX_TRIES);
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

        $outputFile = new CsvFile($filename);
        $numRows = 0;
        $lastFetchedRow = null;
        $colCount = $outputFile->getColumnsCount();
        while ($outputFile->valid()) {
            if (count($outputFile->current()) !== $colCount) {
                throw new BcpAdapterException('The BCP command produced an invalid csv.');
            }
            $lastRow = $outputFile->current();
            $outputFile->next();
            if (!$outputFile->valid()) {
                $lastFetchedRow = $lastRow;
            }
            $numRows++;
        }
        $this->logger->info(sprintf('BCP successfully exported %d rows.', $numRows));
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
        $serverName .= ',' . $this->dbParams['port'];

        $cmd = sprintf(
            'bcp %s queryout %s -S %s -U %s -P %s -d %s -q -k -b50000 -m1 -t, -r"\n" -c',
            escapeshellarg($query),
            escapeshellarg($filename),
            escapeshellarg($serverName),
            escapeshellarg($this->dbParams['user']),
            escapeshellarg($this->dbParams['#password']),
            escapeshellarg($this->dbParams['database'])
        );

        $this->logger->info(sprintf(
            'Executing this BCP command: %s',
            preg_replace('/\-P.*\-d/', '-P ***** -d', $cmd)
        ));
        return $cmd;
    }
}
