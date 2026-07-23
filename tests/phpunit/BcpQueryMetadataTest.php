<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\Adapters\BcpQueryMetadata;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class BcpQueryMetadataTest extends TestCase
{
    public function setUp(): void
    {
        $pdo = PdoTestConnection::createConnection();
        $pdo->exec(
            "IF OBJECT_ID('dbo.bcp_type_normalization', 'U') IS NOT NULL DROP TABLE dbo.bcp_type_normalization",
        );
        $pdo->exec('CREATE TABLE dbo.bcp_type_normalization (
            [id] int NOT NULL,
            [price] decimal(18,2) NULL,
            [name] nvarchar(255) NULL,
            [note] nvarchar(max) NULL,
            [data] varbinary(max) NULL,
            [created] datetime2 NULL,
            [offset] datetimeoffset NULL,
            [t] time NULL,
            [flag] bit NULL,
            [amount] money NULL
        )');
    }

    public function testColumnTypesAreNormalized(): void
    {
        $queryMetadata = new BcpQueryMetadata(
            new MSSQLPdoConnection(new NullLogger(), PdoTestConnection::createDbConfig()),
            'SELECT * FROM dbo.bcp_type_normalization',
        );

        $expected = [
            'id' => ['int', null],
            'price' => ['decimal', '18,2'],
            'name' => ['nvarchar', '255'],
            'note' => ['nvarchar', null],
            'data' => ['varbinary', null],
            'created' => ['datetime2', null],
            'offset' => ['datetimeoffset', null],
            't' => ['time', null],
            'flag' => ['bit', null],
            'amount' => ['money', null],
        ];

        $columns = $queryMetadata->getColumns();
        $this->assertSame(count($expected), $columns->count());

        foreach ($columns as $column) {
            [$expectedType, $expectedLength] = $expected[$column->getName()];
            $this->assertSame(
                $expectedType,
                $column->getType(),
                sprintf('Type of column "%s"', $column->getName()),
            );
            $this->assertSame(
                $expectedLength,
                $column->hasLength() ? $column->getLength() : null,
                sprintf('Length of column "%s"', $column->getName()),
            );
        }
    }
}
