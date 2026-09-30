<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Stubs;

use Keboola\DbExtractor\Adapter\ValueObject\QueryResult;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use LogicException;

/**
 * Records the SQL it is given and replays canned rows, so that the metadata queries can be
 * asserted without a running SQL Server. The parent constructor is deliberately not called --
 * it would open a connection.
 */
class CapturingMssqlPdoConnection extends MSSQLPdoConnection
{
    private const DESCRIPTIONS_MARKER = 'MS_Description';

    private const TABLES_MARKER = '[INFORMATION_SCHEMA].[TABLES]';

    /** @var string[] */
    private array $queries = [];

    /**
     * @param array<array<string, mixed>> $descriptions rows of the MS_Description query
     * @param array<array<string, mixed>> $tables rows of the tables query
     * @param array<array<string, mixed>> $columns rows of the columns query
     */
    public function __construct(
        private array $descriptions = [],
        private array $tables = [],
        private array $columns = [],
    ) {
    }

    public function query(string $query, int $maxRetries = 1, array $values = []): QueryResult
    {
        $this->queries[] = $query;

        if (str_contains($query, self::DESCRIPTIONS_MARKER)) {
            return new StubQueryResult($this->descriptions);
        }

        if (str_contains($query, self::TABLES_MARKER)) {
            return new StubQueryResult($this->tables);
        }

        return new StubQueryResult($this->columns);
    }

    public function quote(string $str): string
    {
        return "'" . $str . "'";
    }

    public function getDescriptionsQuery(): string
    {
        foreach ($this->queries as $query) {
            if (str_contains($query, self::DESCRIPTIONS_MARKER)) {
                return $query;
            }
        }

        throw new LogicException('No descriptions query was executed.');
    }

    /**
     * @return string[]
     */
    public function getQueries(): array
    {
        return $this->queries;
    }
}
