<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\Exception as CsvException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;

class MSSQL extends Extractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_DATETIME = 'datetime';
    public const INCREMENT_TYPE_BINARY = 'binary';
    public const INCREMENT_TYPE_QUOTABLE = 'quotable';

    /** @var  int */
    private $sqlServerVersion;

    /** @var MetadataProvider */
    private $metadataProvider;

    public function __construct(array $parameters, array $state = [], $logger = null)
    {
        parent::__construct($parameters, $state, $logger);

        $this->sqlServerVersion = $this->getSqlServerVersion();
        $this->metadataProvider = new MetadataProvider($this->db);
    }

    private function getSqlServerVersion(): int
    {
        // get the MSSQL Server version (note, 2008 is version 10.*
        $res = $this->db->query("SELECT SERVERPROPERTY('ProductVersion') AS version;");

        $versionString = $res->fetch(\PDO::FETCH_ASSOC);
        if (!isset($versionString['version'])) {
            throw new UserException("Unable to get SQL Server Version Information");
        }
        $versionParts = explode('.', $versionString['version']);
        $this->logger->info(
            sprintf("Found database server version: %s", $versionString['version'])
        );
        return (int) $versionParts[0];
    }

    /**
     * @param array $params
     * @return \PDO
     * @throws UserException
     */
    public function createConnection(array $params): \PDO
    {
        // check params
        if (isset($params['#password'])) {
                $params['password'] = $params['#password'];
        }

        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!array_key_exists($r, $params)) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        // construct DSN connection string
        $host = $params['host'];
        $host .= (isset($params['port']) && $params['port'] !== '1433') ? ',' . $params['port'] : '';
        $host .= empty($params['instance']) ? '' : '\\\\' . $params['instance'];
        $options[] = 'Server=' . $host;
        $options[] = 'Database=' . $params['database'];
        $dsn = sprintf("sqlsrv:%s", implode(';', $options));
        $this->logger->info("Connecting to DSN '" . $dsn . "'");

        // ms sql doesn't support options
        $pdo = new \PDO($dsn, $params['user'], $params['password']);
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        return $pdo;
    }

    public function getConnection(): \PDO
    {
        return $this->db;
    }

    public function testConnection(): void
    {
        $this->db->query('SELECT GETDATE() AS CurrentDateTime')->execute();
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

        $process = new Process($sedCommand);
        $process->setTimeout(300);
        $process->run();
        if ($process->getExitCode() !== 0 || !empty($process->getErrorOutput())) {
            throw new ApplicationException(
                sprintf("Error Stripping Nulls: %s", $process->getErrorOutput())
            );
        }
    }

    private function getLastFetchedDatetimeValue(array $lastExportedLine, array $table, array $columnMetadata): string
    {
        $whereClause = "";
        $whereValues = [];

        foreach ($columnMetadata as $key => $column) {
            if (strtoupper($column['type']) === "TIMESTAMP") {
                continue;
            }
            if ($whereClause !== "") {
                $whereClause .= " AND ";
            }
            if (in_array(strtoupper($column['type']), ["DATETIME", "DATETIME2"])) {
                $whereClause .= "CONVERT(DATETIME2(0), " . $this->quote($column['name']) . ") = ?";
            } else {
                $whereClause .= $this->quote($column['name']) . " = ?";
            }
            $whereValues[] = $lastExportedLine[$key];
        }
        $query = sprintf(
            "SELECT %s FROM %s.%s WHERE %s;",
            $this->quote($this->incrementalFetching['column']),
            $this->quote($table['schema']),
            $this->quote($table['tableName']),
            $whereClause
        );
        $stmt = $this->db->prepare($query);
        $stmt->execute($whereValues);
        $lastDatetimeRow = $stmt->fetch(\PDO::FETCH_ASSOC);
        return $lastDatetimeRow[$this->incrementalFetching['column']];
    }

    private function getLastFetchedId(array $columnMetadata, array $lastExportedLine): string
    {
        $incrementalFetchingColumnIndex = null;
        foreach ($columnMetadata as $key => $column) {
            if ($column['name'] === $this->incrementalFetching['column']) {
                return $lastExportedLine[$key];
            }
        }
    }

    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];
        $csv = $this->createOutputCsv($outputTable);

        $this->logger->info("Exporting to " . $outputTable);

        $columns = $table['columns'];
        $isAdvancedQuery = true;
        $columnMetadata = [];
        if (array_key_exists('table', $table) && !array_key_exists('query', $table)) {
            $isAdvancedQuery = false;
            $tableMetadata = $this->getTables([$table['table']]);
            if (count($tableMetadata) === 0) {
                throw new UserException(sprintf(
                    "Could not find the table: [%s].[%s]",
                    $table['table']['schema'],
                    $table['table']['tableName']
                ));
            }
            $tableMetadata = $tableMetadata[0];
            $columnMetadata = $tableMetadata['columns'];
            if (count($columns) > 0) {
                $columnMetadata = array_filter($columnMetadata, function ($columnMeta) use ($columns) {
                    return in_array($columnMeta['name'], $columns);
                });
                $colOrder = array_flip($columns);
                usort($columnMetadata, function (array $colA, array $colB) use ($colOrder) {
                    return $colOrder[$colA['name']] - $colOrder[$colB['name']];
                });
            }
            $table['table']['nolock'] = $table['nolock'];
            $query = $this->simpleQuery($table['table'], $columnMetadata);
        } else {
            $query = $table['query'];
        }
        $this->logger->debug("Executing query: " . $query);

        try {
            if ($isAdvancedQuery && $this->sqlServerVersion < 11) {
                throw new UserException("BCP is not supported for advanced queries in sql server 2008 or less.");
            }
            $this->logger->info("BCP export started");
            $bcp = new BCP($this->getDbParameters(), $this->logger);
            $exportResult = $bcp->export($query, (string) $csv);
            if ($exportResult['rows'] === 0) {
                // BCP will create an empty file for no rows case
                @unlink((string) $csv);
                // no rows found.  If incremental fetching is turned on, we need to preserve the last state
                if ($this->incrementalFetching['column'] && isset($this->state['lastFetchedRow'])) {
                    $exportResult['lastFetchedRow'] = $this->state['lastFetchedRow'];
                }
                $this->logger->warning(sprintf(
                    "[%s]: Query returned empty result so nothing was imported",
                    $outputTable
                ));
            } else {
                $this->createManifest($table);
                if ($isAdvancedQuery) {
                    $manifestFile = $this->getOutputFilename($table['outputTable']) . '.manifest';
                    $columnsArray = $this->getAdvancedQueryColumns($query);
                    $manifest = json_decode(file_get_contents($manifestFile), true);
                    $manifest['columns'] = $columnsArray;
                    file_put_contents($manifestFile, json_encode($manifest));
                    $this->stripNullBytesInEmptyFields($this->getOutputFilename($table['outputTable']));
                } else if (isset($this->incrementalFetching['column'])) {
                    if ($this->incrementalFetching['type'] === self::INCREMENT_TYPE_DATETIME) {
                        $exportResult['lastFetchedRow'] = $this->getLastFetchedDatetimeValue(
                            $exportResult['lastFetchedRow'],
                            $table['table'],
                            $columnMetadata
                        );
                    } else {
                        $exportResult['lastFetchedRow'] = $this->getLastFetchedId(
                            $columnMetadata,
                            $exportResult['lastFetchedRow']
                        );
                    }
                }
            }
        } catch (\Throwable $e) {
            if ($table['disableFallback']) {
                throw $e;
            }
            $this->logger->info(
                sprintf(
                    "[%s]: The BCP export failed: %s. Attempting export using pdo_sqlsrv.",
                    $outputTable,
                    $e->getMessage()
                )
            );
            try {
                if (!$isAdvancedQuery) {
                    $query = $this->getSimplePdoQuery($table['table'], $columnMetadata);
                }
                $this->logger->info(sprintf("Executing \"%s\" via PDO", $query));
                /** @var \PDOStatement $stmt */
                $stmt = $this->executeQuery(
                    $query,
                    isset($table['retries']) ? (int) $table['retries'] : self::DEFAULT_MAX_TRIES
                );
            } catch (\Exception $e) {
                throw new UserException(
                    sprintf("[%s]: DB query failed: %s.", $outputTable, $e->getMessage()),
                    0,
                    $e
                );
            }
            try {
                $exportResult = $this->writeToCsv($stmt, $csv, $isAdvancedQuery);
                if ($exportResult['rows'] > 0) {
                    $this->createManifest($table);
                } else {
                    if ($this->incrementalFetching['column'] && isset($this->state['lastFetchedRow'])) {
                        $exportResult['lastFetchedRow'] = $this->state['lastFetchedRow'];
                    }
                    $this->logger->warning(sprintf(
                        "[%s]: Query returned empty result so nothing was imported",
                        $outputTable
                    ));
                    @unlink((string) $csv);
                }
            } catch (CsvException $e) {
                throw new ApplicationException("Write to CSV failed: " . $e->getMessage(), 0, $e);
            } catch (\PDOException $PDOException) {
                throw new UserException(
                    "Failed to retrieve results: " . $PDOException->getMessage() . " Code:" . $PDOException->getCode(),
                    0,
                    $PDOException
                );
            }
        }

        $output = [
            "outputTable"=> $outputTable,
            "rows" => $exportResult['rows'],
        ];
        // output state
        if (isset($exportResult['lastFetchedRow']) && !is_array($exportResult['lastFetchedRow'])) {
            $output["state"]['lastFetchedRow'] = $exportResult['lastFetchedRow'];
        }
        return $output;
    }

    /**
     * @param string $query
     * @return array|bool
     * @throws UserException
     */
    public function getAdvancedQueryColumns(string $query)
    {
        // This will only work if the server is >= sql server 2012
        $sql = sprintf(
            "EXEC sp_describe_first_result_set N'%s', null, 0;",
            rtrim(trim(str_replace("'", "''", $query)), ';')
        );
        try {
            /** @var \PDOStatement $stmt */
            $stmt = $this->db->query($sql);
            $result = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            if (is_array($result) && !empty($result)) {
                return array_map(
                    function ($row) {
                        return $row['name'];
                    },
                    $result
                );
            }
            return false;
        } catch (\Exception $e) {
            throw new UserException(
                sprintf('DB query "%s" failed: %s', $sql, $e->getMessage()),
                0,
                $e
            );
        }
    }

    public function getTables(?array $tables = null): array
    {
        return $this->metadataProvider->getTables($tables);
    }

    public function columnToBcpSql(array $column): string
    {
        $datatype = new MssqlDataType(
            $column['type'],
            array_intersect_key($column, array_flip(MssqlDataType::DATATYPE_KEYS))
        );
        $colstr = $escapedColumnName = $this->quote($column['name']);
        if ($datatype->getType() === 'timestamp') {
            $colstr = sprintf('CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), %s), 1)', $colstr);
        } else if ($datatype->getBasetype() === 'STRING') {
            if ($datatype->getType() === 'text'
                || $datatype->getType() === 'ntext'
                || $datatype->getType() === 'xml'
            ) {
                $colstr = sprintf('CAST(%s as nvarchar(max))', $colstr);
            }
            $colstr = sprintf("REPLACE(%s, char(34), char(34) + char(34))", $colstr);
            if ($datatype->isNullable()) {
                $colstr = sprintf("COALESCE(%s,'')", $colstr);
            }
            $colstr = sprintf("char(34) + %s + char(34)", $colstr);
        } else if ($datatype->getBasetype() === 'TIMESTAMP'
            && strtoupper($datatype->getType()) !== 'SMALLDATETIME'
        ) {
            $colstr = sprintf('CONVERT(DATETIME2(0),%s)', $colstr);
        }
        if ($colstr !== $escapedColumnName) {
            return $colstr . ' AS ' . $escapedColumnName;
        }
        return $colstr;
    }

    public function simpleQuery(array $table, array $columns = array()): string
    {
        $queryStart = "SELECT";
        if (isset($this->incrementalFetching['limit'])) {
            $queryStart .= sprintf(
                " TOP %d",
                $this->incrementalFetching['limit']
            );
        }

        $query = sprintf(
            "%s %s FROM %s.%s",
            $queryStart,
            implode(
                ', ',
                array_map(
                    function (array $column): string {
                        return $this->columnToBcpSql($column);
                    },
                    $columns
                )
            ),
            $this->quote($table['schema']),
            $this->quote($table['tableName'])
        );

        if ($table['nolock']) {
            $query .= " WITH(NOLOCK)";
        }
        $incrementalAddon = $this->getIncrementalQueryAddon();
        if ($incrementalAddon) {
            $query .= $incrementalAddon;
        }
        return $query;
    }

    public function columnToPdoSql(array $column): string
    {
        $datatype = new MssqlDataType(
            $column['type'],
            array_intersect_key($column, array_flip(MssqlDataType::DATATYPE_KEYS))
        );
        $colstr = $escapedColumnName = $this->quote($column['name']);
        if ($datatype->getType() === 'timestamp') {
            $colstr = sprintf('CONVERT(NVARCHAR(MAX), CONVERT(BINARY(8), %s), 1)', $colstr);
        } else {
            if ($datatype->getType() === 'text'
                || $datatype->getType() === 'ntext'
                || $datatype->getType() === 'xml'
            ) {
                $colstr = sprintf('CAST(%s as nvarchar(max))', $colstr);
            }
        }
        if ($colstr !== $escapedColumnName) {
            return $colstr . ' AS ' . $escapedColumnName;
        }
        return $colstr;
    }

    public function getSimplePdoQuery(array $table, ?array $columns = []): string
    {
        $queryStart = "SELECT";
        if (isset($this->incrementalFetching['limit'])) {
            $queryStart .= sprintf(
                " TOP %d",
                $this->incrementalFetching['limit']
            );
        }

        if ($columns && count($columns) > 0) {
            $query = sprintf(
                "%s %s FROM %s.%s",
                $queryStart,
                implode(
                    ', ',
                    array_map(
                        function (array $column): string {
                            return $this->columnToPdoSql($column);
                        },
                        $columns
                    )
                ),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            $query = sprintf(
                "%s * FROM %s.%s",
                $queryStart,
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }
        if ($table['nolock']) {
            $query .= " WITH(NOLOCK)";
        }
        $incrementalAddon = $this->getIncrementalQueryAddon();
        if ($incrementalAddon) {
            $query .= $incrementalAddon;
        }
        return $query;
    }

    public static function getColumnMetadata(array $column): array
    {
        $datatype = new MssqlDataType(
            $column['type'],
            array_intersect_key($column, array_flip(self::DATATYPE_KEYS))
        );
        $columnMetadata = $datatype->toMetadata();
        $nonDatatypeKeys = array_diff_key($column, array_flip(self::DATATYPE_KEYS));
        foreach ($nonDatatypeKeys as $key => $value) {
            if ($key === 'name') {
                $columnMetadata[] = [
                    'key' => "KBC.sourceName",
                    'value' => $value,
                ];
            } else {
                $columnMetadata[] = [
                    'key' => "KBC." . $key,
                    'value' => $value,
                ];
            }
        }
        return $columnMetadata;
    }

    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        $query = sprintf(
            "SELECT is_identity, TYPE_NAME(system_type_id) AS data_type 
            FROM sys.columns 
            WHERE object_id = OBJECT_ID('[%s].[%s]') AND sys.columns.name = '%s'",
            $table['schema'],
            $table['tableName'],
            $columnName
        );

        $res = $this->db->query($query);
        $columns = $res->fetchAll();

        if (count($columns) === 0) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $columnName
                )
            );
        }

        $this->incrementalFetching['column'] = $columnName;
        if (in_array($columns[0]['data_type'], MssqlDataType::getNumericTypes())) {
            $this->incrementalFetching['type'] = self::INCREMENT_TYPE_NUMERIC;
        } else if ($columns[0]['data_type'] === 'timestamp') {
            $this->incrementalFetching['type'] = self::INCREMENT_TYPE_BINARY;
        } else if ($columns[0]['data_type'] === 'smalldatetime') {
            $this->incrementalFetching['type'] = self::INCREMENT_TYPE_QUOTABLE;
        } else if (in_array($columns[0]['data_type'], MssqlDataType::TIMESTAMP_TYPES)) {
            $this->incrementalFetching['type'] = self::INCREMENT_TYPE_DATETIME;
        } else {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not numeric or datetime',
                    $columnName
                )
            );
        }
        if ($limit) {
            $this->incrementalFetching['limit'] = $limit;
        }
    }

    private function getIncrementalQueryAddon(): ?string
    {
        $incrementalAddon = null;
        if ($this->incrementalFetching) {
            if (isset($this->state['lastFetchedRow'])) {
                $incrementalAddon = sprintf(
                    " WHERE %s >= %s",
                    $this->quote($this->incrementalFetching['column']),
                    $this->shouldQuoteComparison($this->incrementalFetching['type'])
                        ? $this->db->quote($this->state['lastFetchedRow'])
                        : $this->state['lastFetchedRow']
                );
            }
            $incrementalAddon .= sprintf(" ORDER BY %s", $this->quote($this->incrementalFetching['column']));
        }
        return $incrementalAddon;
    }

    private function shouldQuoteComparison(string $type): bool
    {
        if ($type === self::INCREMENT_TYPE_NUMERIC || $type === self::INCREMENT_TYPE_BINARY) {
            return false;
        }
        return true;
    }

    private function quote(string $obj): string
    {
        return "[{$obj}]";
    }
}
