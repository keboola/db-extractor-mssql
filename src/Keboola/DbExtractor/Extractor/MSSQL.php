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
    public const TYPE_AUTO_INCREMENT = 'autoIncrement';
    public const TYPE_TIMESTAMP = 'timestamp';

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

    private function getLastFetchedDatetimeQuery(array $table, array $columnMetadata): string
    {
        $whereClause = "";
        foreach ($columnMetadata as $key => $column) {
            if ($whereClause !== "") {
                $whereClause .= " AND ";
            }
            if ($column['name'] === $this->incrementalFetching['column']) {
                $whereClause .= "CONVERT(DATETIME2(0), " . $this->quote($column['name']) . ") = ?";
            } else if ($column['type'] === "TIMESTAMP") {
                $whereClause .= $this->quote($column['name']) . " = CONVERT(TIMESTAMP, ?)";
            } else {
                $whereClause .= $this->quote($column['name']) . " = ?";
            }
        }
        return sprintf(
            "SELECT %s FROM %s.%s WHERE %s;",
            $this->quote($this->incrementalFetching['column']),
            $this->quote($table['schema']),
            $this->quote($table['tableName']),
            $whereClause
        );
    }

    private function getLastFetchedDatetimeValue(array $lastExportedLine, string $query): string
    {
        $stmt = $this->db->prepare($query);
        $stmt->execute($lastExportedLine);
        if ($stmt->rowCount() > 1) {
            throw new UserException("Was unable to find unique row for incremental fetching state");
        }
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
            if ($table['nolock']) {
                throw new UserException("Advanced queries do not support the WITH(NOLOCK) option");
            }
        }
        $this->logger->debug("Executing query: " . $query);

        $this->logger->info("BCP export started");
        try {
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
                    if ($this->incrementalFetching['type'] === self::TYPE_TIMESTAMP) {
                        $exportResult['lastFetchedRow'] = $this->getLastFetchedDatetimeValue(
                            $exportResult['lastFetchedRow'],
                            $this->getLastFetchedDatetimeQuery($table['table'], $columnMetadata)
                        );
                    } else if ($this->incrementalFetching['type'] === self::TYPE_AUTO_INCREMENT) {
                        $exportResult['lastFetchedRow'] = $this->getLastFetchedId(
                            $columnMetadata,
                            $exportResult['lastFetchedRow']
                        );
                    }
                }
            }
        } catch (\Throwable $e) {
            $this->logger->info(
                sprintf(
                    "[%s]: The BCP export failed: %s. Attempting export using pdo_sqlsrv.",
                    $outputTable,
                    $e->getMessage()
                )
            );
            try {
                if (!$isAdvancedQuery) {
                    $query = $this->getSimplePdoQuery($table['table'], $columns);
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
        $sql = "SELECT ist.* FROM INFORMATION_SCHEMA.TABLES as ist
                INNER JOIN sys.objects AS so ON ist.TABLE_NAME = so.name
                WHERE (so.type='U' OR so.type='V')";
                // xtype='U' user generated objects only

        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                " AND TABLE_NAME IN (%s) AND TABLE_SCHEMA IN (%s)",
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->db->quote($table['tableName']);
                        },
                        $tables
                    )
                ),
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->db->quote($table['schema']);
                        },
                        $tables
                    )
                )
            );
        }
        $stmt = $this->db->query($sql);

        $arr = $stmt->fetchAll();
        if (count($arr) === 0) {
            return [];
        }

        $tableNameArray = [];
        $tableDefs = [];
        foreach ($arr as $table) {
            $tableNameArray[] = $table['TABLE_NAME'];
            $tableDefs[$table['TABLE_SCHEMA'] . '.' . $table['TABLE_NAME']] = [
                'name' => $table['TABLE_NAME'],
                'catalog' => (isset($table['TABLE_CATALOG'])) ? $table['TABLE_CATALOG'] : '',
                'schema' => (isset($table['TABLE_SCHEMA'])) ? $table['TABLE_SCHEMA'] : '',
                'type' => (isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : '',
            ];
        }
        ksort($tableDefs);

        if (count($tableNameArray) === 0) {
            return [];
        }

        if ($tables === null || count($tables) === 0) {
            $sql = $this->quickTablesSql();
        } else {
            $sql = $this->fullTablesSql($tables);
        }

        $res = $this->db->query($sql);

        $rows = $res->fetchAll();

        foreach ($rows as $i => $column) {
            $curTable = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'];
            if (!array_key_exists('columns', $tableDefs[$curTable])) {
                $tableDefs[$curTable]['columns'] = [];
            }

            $curColumnIndex = $column['ORDINAL_POSITION'] - 1;
            if (!array_key_exists($curColumnIndex, $tableDefs[$curTable]['columns'])) {
                $tableDefs[$curTable]['columns'][$curColumnIndex] = [
                    "name" => $column['COLUMN_NAME'],
                    "sanitizedName" => \Keboola\Utils\sanitizeColumnName($column['COLUMN_NAME']),
                    "type" => $column['DATA_TYPE'],
                    "length" => $this->getFieldLength($column),
                    "nullable" => ($column['IS_NULLABLE'] === "YES" || $column['IS_NULLABLE'] === '1') ? true : false,
                    "ordinalPosition" => (int) $column['ORDINAL_POSITION'],
                    "primaryKey" => false,
                ];
            }

            if (array_key_exists('COLUMN_DEFAULT', $column)) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['default'] = $column['COLUMN_DEFAULT'];
            }

            if (array_key_exists('pk_name', $column) && $column['pk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['primaryKey'] = true;
                $tableDefs[$curTable]['columns'][$curColumnIndex]['primaryKeyName'] = $column['pk_name'];
            }
            if (array_key_exists('is_identity', $column) && $column['is_identity']) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['autoIncrement'] = true;
            }
            if (array_key_exists('uk_name', $column) && $column['uk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['uniqueKey'] = true;
                $tableDefs[$curTable]['columns'][$curColumnIndex]['uniqueKeyName'] = $column['uk_name'];
            }
            if (array_key_exists('chk_name', $column) && $column['chk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]["checkConstraint"] = $column['chk_name'];
                if (isset($column['CHECK_CLAUSE']) && $column['CHECK_CLAUSE'] !== null) {
                    $tableDefs[$curTable]['columns'][$curColumnIndex]["checkClause"] = $column['CHECK_CLAUSE'];
                }
            }
            if (array_key_exists('fk_name', $column) && $column['fk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['foreignKey'] = true;
                $tableDefs[$curTable]['columns'][$curColumnIndex]['foreignKeyName'] = $column['fk_name'];
                $tableDefs[$curTable]['columns'][$curColumnIndex]['foreignKeyRefSchema'] = $column['REFERENCED_SCHEMA_NAME'];
                $tableDefs[$curTable]['columns'][$curColumnIndex]['foreignKeyRefTable'] = $column['REFERENCED_TABLE_NAME'];
                $tableDefs[$curTable]['columns'][$curColumnIndex]['foreignKeyRefColumn'] = $column['REFERENCED_COLUMN_NAME'];
            }
        }
        return array_values($tableDefs);
    }

    private function getFieldLength(array $column): ?string
    {
        $dateTimeTypes = ['datetimeoffset', 'datetime2', 'datetime', 'time', 'smalldatetime', 'date'];
        if (in_array($column['DATA_TYPE'], $dateTimeTypes)) {
            return null;
        }
        if ($column['NUMERIC_PRECISION'] > 0) {
            if ($column['NUMERIC_SCALE'] > 0) {
                return $column['NUMERIC_PRECISION'] . "," . $column['NUMERIC_SCALE'];
            } else {
                return $column['NUMERIC_PRECISION'];
            }
        }
        switch ($column['CHARACTER_MAXIMUM_LENGTH']) {
            case '16':
                // most likely TEXT column
                if ($column['DATA_TYPE'] === 'text') {
                    return null;
                } else {
                    return $column['CHARACTER_MAXIMUM_LENGTH'];
                }
            case '-1':
                // this is returned for max, ex: nvarchar(max), we will treat it as unspecified
                return null;
            default:
                return $column['CHARACTER_MAXIMUM_LENGTH'];
                break;
        }
    }

    private function quickTablesSql(): string
    {
        return "SELECT 
                  OBJECT_SCHEMA_NAME (sys.columns.object_id) AS TABLE_SCHEMA,
                  OBJECT_NAME(sys.columns.object_id) as TABLE_NAME,
                  sys.columns.column_id AS COLUMN_ID,
                  sys.columns.column_id AS ORDINAL_POSITION,
                  sys.columns.name AS COLUMN_NAME,
                  TYPE_NAME(sys.columns.system_type_id) AS DATA_TYPE,
                  sys.columns.is_nullable AS IS_NULLABLE,
                  sys.columns.precision AS NUMERIC_PRECISION,
                  sys.columns.scale AS NUMERIC_SCALE,
                  sys.columns.max_length AS CHARACTER_MAXIMUM_LENGTH,
                  pks.index_name AS pk_name,
                  pks.is_identity AS is_identity
                FROM sys.columns 
                LEFT JOIN
                  (
                    SELECT i.name AS index_name,
                        is_identity,
                        c.column_id AS columnid,
                        c.object_id AS objectid
                    FROM sys.indexes AS i  
                    INNER JOIN sys.index_columns AS ic   
                        ON i.object_id = ic.object_id AND i.index_id = ic.index_id  
                    INNER JOIN sys.columns AS c   
                        ON ic.object_id = c.object_id AND c.column_id = ic.column_id  
                    WHERE i.is_primary_key = 1
                  ) pks 
                ON pks.objectid = sys.columns.object_id AND pks.columnid = sys.columns.column_id
                INNER JOIN sys.objects AS so ON sys.columns.object_id = so.object_id
                WHERE (so.type='U' OR so.type='V')
              ";
    }

    private function fullTablesSql(array $tables): string
    {
        return sprintf(
            "SELECT c.*,  
              chk.CHECK_CLAUSE, 
              fk_name,
              chk_name,
              pk_name,
              uk_name,
              FK_REFS.REFERENCED_COLUMN_NAME, 
              FK_REFS.REFERENCED_TABLE_NAME,
              FK_REFS.REFERENCED_SCHEMA_NAME
            FROM INFORMATION_SCHEMA.COLUMNS AS c 
            LEFT JOIN (
                SELECT  
                     KCU1.CONSTRAINT_NAME AS fk_name 
                    ,KCU1.CONSTRAINT_SCHEMA AS FK_SCHEMA_NAME
                    ,KCU1.TABLE_NAME AS FK_TABLE_NAME 
                    ,KCU1.COLUMN_NAME AS FK_COLUMN_NAME 
                    ,KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION 
                    ,KCU2.CONSTRAINT_NAME AS REFERENCED_CONSTRAINT_NAME 
                    ,KCU2.CONSTRAINT_SCHEMA AS REFERENCED_SCHEMA_NAME
                    ,KCU2.TABLE_NAME AS REFERENCED_TABLE_NAME 
                    ,KCU2.COLUMN_NAME AS REFERENCED_COLUMN_NAME 
                    ,KCU2.ORDINAL_POSITION AS REFERENCED_ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC 
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1 
                    ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
                    AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
                    AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2 
                    ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG  
                    AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA 
                    AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME 
                    AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION 
            ) AS FK_REFS
            ON FK_REFS.FK_TABLE_NAME = c.TABLE_NAME AND FK_REFS.FK_COLUMN_NAME = c.COLUMN_NAME
            LEFT JOIN (
                SELECT tc2.CONSTRAINT_TYPE, tc2.TABLE_NAME, ccu2.COLUMN_NAME, ccu2.CONSTRAINT_NAME as chk_name, CHK.CHECK_CLAUSE 
                FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu2 
                JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc2 
                ON ccu2.TABLE_NAME = tc2.TABLE_NAME
                JOIN (
                  SELECT * FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS 
                ) AS CHK 
                ON CHK.CONSTRAINT_NAME = ccu2.CONSTRAINT_NAME
                WHERE CONSTRAINT_TYPE = 'CHECK'
            ) AS chk
            ON chk.TABLE_NAME = c.TABLE_NAME AND chk.COLUMN_NAME = c.COLUMN_NAME
            LEFT JOIN (
                SELECT tc.CONSTRAINT_TYPE, tc.TABLE_NAME, ccu.COLUMN_NAME, ccu.CONSTRAINT_NAME as pk_name
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ccu
                JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND  ccu.TABLE_NAME = tc.TABLE_NAME AND CONSTRAINT_TYPE = 'PRIMARY KEY' 
            ) AS pk
            ON pk.TABLE_NAME = c.TABLE_NAME AND pk.COLUMN_NAME = c.COLUMN_NAME
            LEFT JOIN (
                SELECT tc.CONSTRAINT_TYPE, ccu.TABLE_NAME, ccu.COLUMN_NAME, ccu.CONSTRAINT_NAME as uk_name
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ccu
                JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND ccu.TABLE_NAME = tc.TABLE_NAME AND CONSTRAINT_TYPE = 'UNIQUE' 
            ) AS uk  
            ON uk.TABLE_NAME = c.TABLE_NAME AND uk.COLUMN_NAME = c.COLUMN_NAME
            WHERE c.TABLE_NAME IN (%s) AND c.TABLE_SCHEMA IN (%s)
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, ORDINAL_POSITION",
            implode(
                ',',
                array_map(
                    function ($table) {
                        return $this->db->quote($table['tableName']);
                    },
                    $tables
                )
            ),
            implode(
                ',',
                array_map(
                    function ($table) {
                        return $this->db->quote($table['schema']);
                    },
                    $tables
                )
            )
        );
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

        $datatypeKeys = ['type', 'length', 'nullable', 'default', 'format'];
        $query = sprintf(
            "%s %s FROM %s.%s",
            $queryStart,
            implode(
                ', ',
                array_map(
                    function ($column) use ($datatypeKeys) {
                        $datatype = new GenericStorage(
                            $column['type'],
                            array_intersect_key($column, array_flip($datatypeKeys))
                        );
                        $colstr = $this->quote($column['name']);
                        if ($datatype->getBasetype() === 'STRING') {
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
                            && strtoupper($datatype->getType()) !== 'TIMESTAMP'
                            && strtoupper($datatype->getType()) !== 'SMALLDATETIME'
                        ) {
                            $colstr = sprintf('CONVERT(DATETIME2(0),%s)', $colstr);
                        }
                        return $colstr;
                    },
                    $columns
                )
            ),
            $this->quote($table['schema']),
            $this->quote($table['tableName'])
        );

        $incrementalAddon = $this->getIncrementalQueryAddon();
        if ($incrementalAddon) {
            $query .= $incrementalAddon;
        }
        if ($table['nolock']) {
            $query .= " WITH(NOLOCK)";
        }
        return $query;
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
                implode(', ', array_map(function ($column) {
                    return $this->quote($column);
                }, $columns)),
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
        $incrementalAddon = $this->getIncrementalQueryAddon();
        if ($incrementalAddon) {
            $query .= $incrementalAddon;
        }
        if ($table['nolock']) {
            $query .= " WITH(NOLOCK)";
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

        if ($columns[0]['is_identity']) {
            $this->incrementalFetching['column'] = $columnName;
            $this->incrementalFetching['type'] = self::TYPE_AUTO_INCREMENT;
        } else if ($columns[0]['data_type'] === 'datetime' || $columns[0]['data_type'] === 'datetime2') {
            $this->incrementalFetching['column'] = $columnName;
            $this->incrementalFetching['type'] = self::TYPE_TIMESTAMP;
        } else {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not an identity column or a datetime',
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
                if ($this->incrementalFetching['type'] === self::TYPE_AUTO_INCREMENT) {
                    $incrementalAddon = sprintf(
                        ' WHERE %s > %d',
                        $this->quote($this->incrementalFetching['column']),
                        (int) $this->state['lastFetchedRow']
                    );
                } else if ($this->incrementalFetching['type'] === self::TYPE_TIMESTAMP) {
                    $incrementalAddon = sprintf(
                        " WHERE %s > %s",
                        $this->quote($this->incrementalFetching['column']),
                        $this->db->quote($this->state['lastFetchedRow'])
                    );
                } else {
                    throw new ApplicationException(
                        sprintf('Unknown incremental fetching column type %s', $this->incrementalFetching['type'])
                    );
                }
            }
            $incrementalAddon .= sprintf(" ORDER BY %s", $this->quote($this->incrementalFetching['column']));
        }
        return $incrementalAddon;
    }

    private function quote(string $obj): string
    {
        return "[{$obj}]";
    }
}
