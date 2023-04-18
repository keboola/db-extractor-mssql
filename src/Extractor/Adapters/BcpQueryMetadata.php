<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor\Adapters;

use Keboola\DbExtractor\Adapter\Exception\UserException;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\Exception\BcpAdapterException;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
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
            rtrim(trim(str_replace("'", "''", $this->query)), ';')
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
                        var_export($result, true)
                    ));
                }
                $builder->setName($columnMetadata['name']);
                $builder->setType($columnMetadata['system_type_name']);
                $columns[] = $builder->build();
            }
            return new ColumnCollection($columns);
        } catch (Throwable $e) {
            throw new BcpAdapterException(
                sprintf('DB query "%s" failed: %s', $sql, $e->getMessage()),
                0,
                $e
            );
        }
    }
}
