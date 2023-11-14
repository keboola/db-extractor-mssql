<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Adapter\Exception\UserException;
use Keboola\DbExtractor\Extractor\Adapters\BcpQueryMetadata;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use PDOException;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use ReflectionClass;

class ExceptionHandlingTest extends TestCase
{
    public function setUp(): void
    {
        PdoTestConnection::createConnection();
    }

    public function testBcpQueryMetadataExceptionHandling(): void
    {
        $this->expectException(UserException::class);
        // phpcs:disable Generic.Files.LineLength
        $this->expectExceptionMessage("Cannot retrieve column metadata via query \"EXEC sp_describe_first_result_set N'SET NOCOUNT ON  EXEC Keboola_propojeni.dbo.IFS_ImportObratovky_All 129;  SELECT ''OK'' AS Status', null, 0;\". The metadata could not be determined because statement 'delete from #ErrFile' in procedure 'IFS_ImportObratovky_All' uses a temp table.");
        // phpcs:enable Generic.Files.LineLength

        $query = "EXEC sp_describe_first_result_set N'SET NOCOUNT ON  " .
            "EXEC Keboola_propojeni.dbo.IFS_ImportObratovky_All 129;  SELECT ''OK'' AS Status', null, 0;";

        $object = new BcpQueryMetadata(
            new MSSQLPdoConnection(new NullLogger(), PdoTestConnection::createDbConfig()),
            $query,
        );
        $method = (new ReflectionClass($object))->getMethod('handleException');
        $method->setAccessible(true);
        throw $method->invoke(
            $object,
            new PDOException('SQLSTATE[42000]: [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]The ' .
                "metadata could not be determined because statement 'delete from #ErrFile' in procedure 'IFS_Import" .
                'Obratovky_All\' uses a temp table.'),
            $query,
        );
    }
}
