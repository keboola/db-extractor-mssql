<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor\Adapters;

use Keboola\DbExtractor\Adapter\Exception\UserException;
use Keboola\DbExtractor\Adapter\PDO\PdoQueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Exception\InvalidArgumentException;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use PDOStatement;

/**
 * Reports an unusable column name from PDO query metadata as a user error.
 *
 * The parent builds the columns from PDOStatement::getColumnMeta(). When the query result
 * contains a column with an empty name - typically a computed column of an advanced query
 * that has no alias - ColumnBuilder throws a TableResultFormat InvalidArgumentException,
 * which is neither a user nor an application exception, so the job dies with an opaque
 * "internal error" (exit code 2) and tells the user nothing.
 *
 * getColumns() is called lazily by the manifest generator, i.e. after the export adapter
 * has already returned, so nothing else re-classifies the exception afterwards.
 *
 * The user can fix this by naming the column, so keep the same failure but report it as a
 * user error (exit code 1) with the query in the message - the same treatment
 * BcpQueryMetadata gives its own metadata failures.
 */
class MSSQLPdoQueryMetadata extends PdoQueryMetadata
{
    private string $query;

    public function __construct(PDOStatement $stmt, string $query)
    {
        parent::__construct($stmt);
        $this->query = $query;
    }

    public function getColumns(): ColumnCollection
    {
        try {
            return parent::getColumns();
        } catch (InvalidArgumentException $e) {
            throw new UserException(
                sprintf(
                    'Cannot retrieve column metadata via query "%s". %s Make sure every column of the '
                    . 'query result has a name, e.g. by adding an alias to computed columns.',
                    $this->query,
                    $e->getMessage(),
                ),
                0,
                $e,
            );
        }
    }
}
