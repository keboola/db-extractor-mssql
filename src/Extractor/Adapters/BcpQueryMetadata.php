<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor\Adapters;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\DbExtractor\Adapter\Exception\UserException;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\Exception\BcpAdapterException;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use Keboola\DbExtractor\Metadata\SystemTypeName;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use Throwable;

class BcpQueryMetadata implements QueryMetadata
{
    private MSSQLPdoConnection $connection;

    private string $query;

    public function __construct(MSSQLPdoConnection $connection, string $query)
    {
        $this->connection = $connection;
        $this->query = $query;
    }

    public function getColumns(): ColumnCollection
    {
        // This will only work if the server is >= sql server 2012
        $sql = sprintf(
            "EXEC sp_describe_first_result_set N'%s', null, 0;",
            rtrim(trim(str_replace("'", "''", $this->query)), ';'),
        );
        try {
            $result = $this->connection->query($sql)->fetchAll();
            $columns = [];
            foreach ($result as $columnMetadata) {
                $builder = ColumnBuilder::create();
                if (!isset($columnMetadata['name']) || !isset($columnMetadata['system_type_name'])) {
                    throw new UserException(sprintf(
                        'Cannot retrieve all column metadata via query "%s". Result: %s',
                        $sql,
                        var_export($result, true),
                    ));
                }
                $builder->setName($columnMetadata['name']);
                $systemTypeName = SystemTypeName::parse($columnMetadata['system_type_name']);
                $builder->setType($systemTypeName->getType());
                $builder->setLength($systemTypeName->getLength());
                $columns[] = $builder->build();
            }
            return new ColumnCollection($columns);
        } catch (Throwable $e) {
            throw $this->handleException($e, $sql);
        }
    }

    protected function handleException(Throwable $e, string $sql): Throwable
    {
        if (strpos($e->getMessage(), 'uses a temp table') !== false) {
            preg_match('/\[SQL Server\](.*)/', $e->getMessage(), $matches);
            return new UserException(
                sprintf(
                    'Cannot retrieve column metadata via query "%s". %s',
                    $sql,
                    $matches[1] ?? $e->getMessage(),
                ),
                0,
                $e,
            );
        }
        // The connection layer already retries transient DB errors (PDOException) with
        // exponential backoff and, once the retries are exhausted, reports them as a user
        // exception - e.g. UserRetriedException for "Login timeout expired". Re-wrapping an
        // exception that is already classified as user-facing into an ApplicationException
        // downgrades it to an opaque "internal error" (exit code 2). Keep the original
        // classification so the job exits 1 with the message the user can act on.
        if ($e instanceof UserExceptionInterface) {
            return new UserException(
                sprintf('DB query "%s" failed: %s', $sql, $e->getMessage()),
                0,
                $e,
            );
        }

        return new BcpAdapterException(
            sprintf('DB query "%s" failed: %s', $sql, $e->getMessage()),
            0,
            $e,
        );
    }
}
