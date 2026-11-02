<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;

/**
 * Carries an already resolved ColumnCollection, so a resolved collection can be passed to the
 * manifest generator in place of the query metadata it came from.
 */
class ColumnCollectionQueryMetadata implements QueryMetadata
{
    private ColumnCollection $columns;

    public function __construct(ColumnCollection $columns)
    {
        $this->columns = $columns;
    }

    public function getColumns(): ColumnCollection
    {
        return $this->columns;
    }
}
