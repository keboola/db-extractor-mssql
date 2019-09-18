<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor\DbAdapter;

use Keboola\DbExtractor\Exception\UserException;
use PDO;

class MssqlAdapter extends PDO
{
    public function testConnection(): void
    {
        $this->query('SELECT GETDATE() AS CurrentDateTime')->execute();
    }

    public function quoteIdentifier(string $obj): string
    {
        return "[{$obj}]";
    }

    public function fetchServerVersion(): string
    {
        // get the MSSQL Server version (note, 2008 is version 10.*
        $res = $this->query("SELECT SERVERPROPERTY('ProductVersion') AS version;");

        $versionString = $res->fetch(\PDO::FETCH_ASSOC);
        if (!isset($versionString['version'])) {
            throw new UserException('Unable to get SQL Server Version Information');
        }
        return $versionString['version'];
    }
}
