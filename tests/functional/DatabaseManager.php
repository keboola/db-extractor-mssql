<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SalesTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SimpleTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SpecialTableTrait;
use \PDO;

class DatabaseManager
{
    use SimpleTableTrait;
    use AutoIncrementTableTrait;
    use SalesTableTrait;
    use SpecialTableTrait;

    protected PDO $connection;

    public function __construct(PDO $connection)
    {
        $this->connection = $connection;
    }
}
