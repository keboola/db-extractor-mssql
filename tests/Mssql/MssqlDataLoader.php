<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Mssql\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Test\AbstractExtractorTest;
use Keboola\DbExtractor\Test\AbstractPdoDataLoader;
use UnexpectedValueException;

class MssqlDataLoader extends AbstractPdoDataLoader
{
    public function createAndUseDb(string $database): void
    {
        $this->executeQuery('USE master');
        $quotedDb = $this->quoteIdentifier($database);
        $this->executeQuery(sprintf("
            IF NOT EXISTS(select * from sys.databases where name='%s') 
            CREATE DATABASE %s
        ", $database, $quotedDb));
        $this->executeQuery(sprintf("USE %s", $quotedDb));
        $this->executeQuery(sprintf(
            "IF NOT EXISTS(select * from sys.schemas where name='%s') 
            BEGIN
             EXEC('CREATE SCHEMA %s AUTHORIZATION [dbo]')
            END",
            $this->getSchema(),
            $this->quoteIdentifier($this->getSchema())
        ));
    }

    protected function quoteIdentifier(string $identifier): string
    {
        return '[' . $identifier . ']';
    }

    public function load(string $inputFile, string $destinationTable, int $ignoreLines = 1): void
    {
        $csv = new CsvFile($inputFile);
        $rows = iterator_to_array($csv);
        if ($ignoreLines === 1) {
            $header = array_shift($rows);

            foreach ($rows as $key => $row) {
                $rows[$key] = array_combine($header, $row);
            }
        }
        $this->addRows($destinationTable, $rows);
    }

    protected function generateColumnDefinition(
        string $columnName,
        string $columnType,
        ?string $columnLength,
        ?bool $columnNullable,
        ?string $columnDefault,
        ?bool $isPrimary
    ): string {
        $result = $this->quoteIdentifier($columnName) . ' ';
        switch ($columnType) {
            case AbstractExtractorTest::COLUMN_TYPE_VARCHAR:
                $result .= 'nvarchar';
                break;
            case AbstractExtractorTest::COLUMN_TYPE_INTEGER:
                $result .= 'int';
                break;
            case AbstractExtractorTest::COLUMN_TYPE_AUTOUPDATED_TIMESTAMP:
                $result .= 'DATETIME2 NULL DEFAULT GETDATE()';
                return $result;
            default:
                throw new UnexpectedValueException(sprintf('Unknown column type %s', $columnType));
        }
        if ($columnLength > 0) {
            $result .= '(' . $columnLength . ')';
        }
        if ($columnNullable !== null) {
            $nullable = $columnNullable;
            if ($nullable === true) {
                $result .= ' NULL ';
            } elseif ($nullable === false) {
                $result .= ' NOT NULL ';
            }
        }
        if ($columnDefault !== null) {
            $default = $columnDefault;
            if ($default) {
                $result .= 'DEFAULT ' . $this->quote($default);
            }
        }

        return $result;
    }

    protected function getForeignKeySqlString(
        string $quotedTableName,
        string $quotedColumnsString,
        string $quotedReferenceColumnsString
    ): string {
        $fkName = 'fk_' . str_replace([
                '[',
                ']',
            ], '_', $quotedTableName . $quotedColumnsString . $quotedReferenceColumnsString);
        $quotedSchemaName = $this->quoteIdentifier($this->getSchema());
        return sprintf(
            '
CONSTRAINT %s FOREIGN KEY (%s)     
    REFERENCES %s.%s (%s)     
    ON DELETE CASCADE    
    ON UPDATE CASCADE
            ',
            $this->quoteIdentifier($fkName),
            $quotedColumnsString,
            $quotedSchemaName,
            $quotedTableName,
            $quotedReferenceColumnsString
        );
    }

    protected function getPrimaryKeySqlString(string $primaryKeyColumnsString): string
    {
        return sprintf('PRIMARY KEY (%s)', $primaryKeyColumnsString);
    }

    protected function getCreateTableQuery(
        string $quotedTableName,
        string $columnsDefinition,
        string $primaryKeyDefinition,
        string $foreignKeyDefintion
    ): string {
        $quotedSchema = $this->quoteIdentifier($this->getSchema());
        return sprintf(
            '
CREATE TABLE %s.%s (
    %s
    %s
    %s
            )
            ',
            $quotedSchema,
            $quotedTableName,
            $columnsDefinition,
            $primaryKeyDefinition,
            $foreignKeyDefintion
        );
    }

    protected function getInsertSqlQuery(
        string $quotedTableName,
        ?string $quotedTableColumnsSqlString,
        string $valuesString
    ): string {
        $quotedSchemaName = $this->quoteIdentifier($this->getSchema());
        $query = sprintf(
            'INSERT INTO %s.%s %s VALUES %s',
            $quotedSchemaName,
            $quotedTableName,
            $quotedTableColumnsSqlString === null ? '' : '(' . $quotedTableColumnsSqlString . ')',
            $valuesString
        );
        return $query;
    }

    protected function quote(string $string): string
    {
        return 'N' . parent::quote($string);
    }

    protected function getDropTableSqlQuery(string $quotedTableName): string
    {
        $quotedSchema = $this->quoteIdentifier($this->getSchema());
        return sprintf(
            "IF OBJECT_ID('%s.%s', 'U') IS NOT NULL DROP TABLE %s.%s",
            $quotedSchema,
            $quotedTableName,
            $quotedSchema,
            $quotedTableName
        );
    }

    public function dropTable(string $tableName): void
    {
        $relatedTablesQuery = sprintf("
SELECT * 
FROM sys.foreign_keys
WHERE referenced_object_id = object_id('%s')
        ", $tableName);
        $stmt = $this->db->query($relatedTablesQuery);
        $relatedTables = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($relatedTables as $relatedTable) {
            $name = $this->db
                ->query(
                    sprintf('SELECT OBJECT_NAME(%s)', $this->quote($relatedTable['parent_object_id']))
                )->fetch(\PDO::FETCH_COLUMN);
            $this->dropTable($name);
        }
        parent::dropTable($tableName);
    }

    /**
     * @return string
     */
    protected function getSchema(): string
    {
        return 'dbo';
    }
}
