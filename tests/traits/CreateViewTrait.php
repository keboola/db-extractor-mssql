<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use PDO;

trait CreateViewTrait
{
    use QuoteIdentifierTrait;

    protected Pdo $connection;

    public function createView(string $viewName, string $fromTable): void
    {
        // Create view
        $this->connection->prepare(sprintf(
            'create view %s as select * from %s',
            $this->quoteIdentifier($viewName),
            $this->quoteIdentifier($fromTable),
        ))->execute();
    }
}
