<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DbExtractor\TraitTests\CreateViewTrait;
use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\NullableTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SalesTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SimpleTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SpecialTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\TimestampTableTrait;
use PDO;

class DatabaseManager
{
    use CreateViewTrait;
    use SimpleTableTrait;
    use AutoIncrementTableTrait;
    use SalesTableTrait;
    use SpecialTableTrait;
    use TimestampTableTrait;
    use NullableTableTrait;

    protected PDO $connection;

    public function __construct(PDO $connection)
    {
        $this->connection = $connection;
    }
}
