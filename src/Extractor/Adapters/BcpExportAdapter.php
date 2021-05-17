<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor\Adapters;

use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Exception\BcpAdapterSkippedException;
use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\MSSQLQueryFactory;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Throwable;
use Psr\Log\LoggerInterface;
use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\BcpAdapterException;
use Keboola\DbExtractor\Extractor\MssqlDataType;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use Symfony\Component\Process\Process;

class BcpExportAdapter implements ExportAdapter
{
    protected MSSQLQueryFactory $simpleQueryFactory;

    private MSSQLPdoConnection $connection;

    private MetadataProvider $metadataProvider;

    private DatabaseConfig $databaseConfig;

    private LoggerInterface $logger;

    public function __construct(
        LoggerInterface $logger,
        MSSQLPdoConnection $connection,
        MetadataProvider $metadataProvider,
        DatabaseConfig $databaseConfig,
        MSSQLQueryFactory $queryFactory
    ) {
        $this->logger = $logger;
        $this->connection = $connection;
        $this->metadataProvider = $metadataProvider;
        $this->databaseConfig = $databaseConfig;
        $this->simpleQueryFactory = $queryFactory;
    }

    public function getName(): string
    {
        return 'BCP';
    }

    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        if (!$exportConfig instanceof MssqlExportConfig) {
            throw new InvalidArgumentException('PgsqlExportConfig expected.');
        }

        if ($exportConfig->isBcpDisabled()) {
            throw new BcpAdapterSkippedException('Disabled in configuration.');
        }

        if ($exportConfig->hasQuery() && $this->connection->getServerVersion() < 11) {
            throw new BcpAdapterSkippedException(
                'BCP is not supported for advanced queries in sql server 2008 or less.'
            );
        }

        $query = $exportConfig->hasQuery() ? $exportConfig->getQuery() : $this->createSimpleQuery($exportConfig);

        try {
            $exportResult = $this->doExport(
                $exportConfig,
                $query,
                $csvFilePath
            );
            if ($exportResult->getRowsCount() > 0 && $exportConfig->hasQuery()) {
                $this->stripNullBytesInEmptyFields($csvFilePath);
            }
            return $exportResult;
        } catch (BcpAdapterException $pdoError) {
            @unlink($csvFilePath);
            throw new UserException($pdoError->getMessage());
        }
    }

    protected function createSimpleQuery(ExportConfig $exportConfig): string
    {
        return $this
            ->simpleQueryFactory
            ->setFormat(MSSQLQueryFactory::ESCAPING_TYPE_BCP)
            ->create($exportConfig, $this->connection);
    }

    private function getLastDatetimeValue(ExportConfig $exportConfig, array $lastRow): string
    {
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

            // COALESCE note: NULL is exported as empty string to $lastRow, so
            // ... COALESCE is required, because NULL = "" -> false
            // ... COALESCE(NULL, "") = "" -> true
            if (in_array(strtoupper($column->getType()), ['DATETIME', 'DATETIME2'])) {
                $whereClause .=
                    'COALESCE(CONVERT(DATETIME2(0), ' .
                    $this->connection->quoteIdentifier($column->getName()) .
                    '), \'\') = ?';
            } else {
                $whereClause .= 'COALESCE(' . $this->connection->quoteIdentifier($column->getName()) . ', \'\') = ?';
            }

            $whereValues[] = $lastRow[$key];
        }

        $query = sprintf(
            'SELECT %s FROM %s.%s WHERE %s;',
            $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->connection->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->connection->quoteIdentifier($exportConfig->getTable()->getName()),
            $whereClause
        );

        $result = $this->connection->query($query, $exportConfig->getMaxRetries(), $whereValues)->fetchAll();

        if (count($result) > 0) {
            return $result[0][$exportConfig->getIncrementalFetchingColumn()];
        }

        throw new BcpAdapterException('Fetching last datetime value returned no results.');
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

        throw new BcpAdapterException('Fetching last id value returned no results.');
    }

    private function doExport(MssqlExportConfig $exportConfig, string $query, string $filename): ExportResult
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
            return $this->processOutputCsv($exportConfig, $filename, $query, $process);
        } catch (BcpAdapterException $e) {
            throw $e;
        } catch (Throwable $e) {
            throw new BcpAdapterException(
                'The BCP command produced an invalid csv: ' . $e->getMessage(),
                0,
                $e
            );
        }
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
        $process->setTimeout(3600);
        $process->run();
        if ($process->getExitCode() !== 0 || !empty($process->getErrorOutput())) {
            throw new ApplicationException(
                sprintf('Error Stripping Nulls: %s', $process->getErrorOutput())
            );
        }
    }

    private function processOutputCsv(
        MssqlExportConfig $exportConfig,
        string $filename,
        string $query,
        Process $process
    ): ExportResult {
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
                    'bcpErrorOutput' => mb_convert_encoding($process->getErrorOutput(), 'UTF-8', 'UTF-8'),
                ]);
            }
            $lastRow = $outputFile->current();
            $outputFile->next();
            if (!$outputFile->valid()) {
                $lastFetchedRow = $lastRow;
            }
            $numRows++;
        }

        $lastFetchedRowMaxValue = null;

        // Find max value only if BaseExtractor::canFetchMaxIncrementalValueSeparately == false
        if (!$exportConfig->hasQuery() &&
            $exportConfig->isIncrementalFetching() &&
            $exportConfig->hasIncrementalFetchingLimit() &&
            $lastFetchedRow
        ) {
            if ($this->simpleQueryFactory->getIncrementalFetchingType() === MssqlDataType::INCREMENT_TYPE_DATETIME) {
                $lastFetchedRowMaxValue = $this->getLastDatetimeValue($exportConfig, $lastFetchedRow);
            } else {
                $lastFetchedRowMaxValue = $this->getLastValue($exportConfig, $lastFetchedRow);
            }
        }

        return new ExportResult(
            $filename,
            $numRows,
            new BcpQueryMetadata($this->connection, $query),
            false,
            $lastFetchedRowMaxValue ?? null
        );
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

        $commandForLogger = preg_replace('/-P.*-d/', '-P ***** -d', $cmd);
        $commandForLogger = preg_replace(
            '/queryout.*\/([a-z\-._]+\.csv).*-S/',
            'queryout \'${1}\' -S',
            (string) $commandForLogger
        );

        $this->logger->info(sprintf(
            'Executing BCP command: %s',
            $commandForLogger
        ));
        return $cmd;
    }
}
