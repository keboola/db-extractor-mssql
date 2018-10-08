<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\Exception as CsvException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Symfony\Component\Process\Process;

class MSSQL extends Extractor
{
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

    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];
        $csv = $this->createOutputCsv($outputTable);

        $this->logger->info("Exporting to " . $outputTable);

        $columns = $table['columns'];
        $isAdvancedQuery = true;
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
            $query = $this->simpleQuery($table['table'], $columnMetadata);
        } else {
            $query = $table['query'];
        }
        $this->logger->debug("Executing query: " . $query);

        $this->logger->info("BCP export started");
        try {
            $bcp = new BCP($this->getDbParameters(), $this->logger);
            $numRows = $bcp->export($query, (string) $csv);
            if ($numRows === 0) {
                // BCP will create an empty file for no rows case
                @unlink((string) $csv);
                $this->logger->warning(sprintf(
                    "Query returned empty result. Nothing was imported for table [%s]",
                    $table['name']
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
                }
            }
        } catch (\Throwable $e) {
            $this->logger->info(
                sprintf(
                    "[%s]: BCP command failed: %s. Attempting export using pdo_sqlsrv.",
                    $table['name'],
                    $e->getMessage()
                )
            );
            try {
                if (!$isAdvancedQuery) {
                    $query = $this->simplePdoQuery($table['table'], $columns);
                }
                $this->logger->info(sprintf("Executing \"%s\" via PDO", $query));
                /** @var \PDOStatement $stmt */
                $stmt = $this->executeQuery(
                    $query,
                    isset($table['retries']) ? (int) $table['retries'] : self::DEFAULT_MAX_TRIES
                );
            } catch (\Exception $e) {
                throw new UserException(
                    sprintf("[%s]: DB query failed: %s.", $table['name'], $e->getMessage()),
                    0,
                    $e
                );
            }
            try {
                $result = $this->writeToCsv($stmt, $csv, $isAdvancedQuery);
                $numRows = $result['rows'];
                if ($numRows > 0) {
                    $this->createManifest($table);
                } else {
                    $this->logger->warning(sprintf(
                        "Query returned empty result. Nothing was imported for table [%s]",
                        $table['name']
                    ));
                    @unlink((string) $csv);
                }
            } catch (CsvException $e) {
                throw new ApplicationException("Write to CSV failed: " . $e->getMessage(), 0, $e);
            }
        }

        $output = [
            "outputTable"=> $outputTable,
            "rows" => $numRows,
        ];
        // output state
        if (!empty($result['lastFetchedRow'])) {
            $output["state"]['lastFetchedRow'] = $result['lastFetchedRow'];
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
                INNER JOIN sysobjects AS so ON ist.TABLE_NAME = so.name
                WHERE (so.xtype='U' OR so.xtype='V') AND so.name NOT IN ('sysconstraints', 'syssegments')";
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
            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if ($column['NUMERIC_SCALE'] > 0) {
                    $length = $column['NUMERIC_PRECISION'] . "," . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }
            $curColumnIndex = $column['ORDINAL_POSITION'] - 1;
            if (!array_key_exists($curColumnIndex, $tableDefs[$curTable]['columns'])) {
                $tableDefs[$curTable]['columns'][$curColumnIndex] = [
                    "name" => $column['COLUMN_NAME'],
                    "sanitizedName" => \Keboola\Utils\sanitizeColumnName($column['COLUMN_NAME']),
                    "type" => $column['DATA_TYPE'],
                    "length" => $length,
                    "nullable" => ($column['IS_NULLABLE'] === "YES") ? true : false,
                    "default" => $column['COLUMN_DEFAULT'],
                    "ordinalPosition" => $column['ORDINAL_POSITION'],
                    "primaryKey" => false,
                ];
            }

            if (array_key_exists('pk_name', $column) && $column['pk_name'] !== null) {
                $tableDefs[$curTable]['columns'][$curColumnIndex]['primaryKey'] = true;
                $tableDefs[$curTable]['columns'][$curColumnIndex]['primaryKeyName'] = $column['pk_name'];
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

    private function quickTablesSql(): string
    {
        return "SELECT c.*, pk_name 
                FROM INFORMATION_SCHEMA.COLUMNS AS c
                INNER JOIN sysobjects AS so ON c.TABLE_NAME = so.name AND (so.xtype='U' OR so.xtype='V') AND so.name NOT IN ('sysconstraints', 'syssegments')
                LEFT JOIN (
                    SELECT tc.CONSTRAINT_TYPE, tc.TABLE_NAME, ccu.COLUMN_NAME, ccu.CONSTRAINT_NAME as pk_name
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ccu
                    JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                    ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND  ccu.TABLE_NAME = tc.TABLE_NAME AND CONSTRAINT_TYPE = 'PRIMARY KEY' 
                ) AS pk
                ON pk.TABLE_NAME = c.TABLE_NAME AND pk.COLUMN_NAME = c.COLUMN_NAME";
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
        $datatypeKeys = ['type', 'length', 'nullable', 'default', 'format'];
        return sprintf(
            "SELECT %s FROM %s.%s",
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
                        } else if ($datatype->getBasetype() === 'TIMESTAMP' && strtoupper($datatype->getType()) !== 'TIMESTAMP') {
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
    }

    public function simplePdoQuery(array $table, array $columns = array()): string
    {
        if (count($columns) > 0) {
            return sprintf("SELECT %s FROM %s.%s",
                implode(', ', array_map(function ($column) {
                    return $this->quote($column);
                }, $columns)),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            return sprintf("SELECT * FROM %s.%s", $this->quote($table['schema']), $this->quote($table['tableName']));
        }
    }

    private function quote(string $obj): string
    {
        return "[{$obj}]";
    }
}
