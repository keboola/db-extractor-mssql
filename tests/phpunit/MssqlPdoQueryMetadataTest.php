<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\MssqlPdoQueryMetadata;
use Keboola\DbExtractor\FunctionalTests\PdoTestConnection;
use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;

class MssqlPdoQueryMetadataTest extends TestCase
{
    private const QUERY = 'SELECT * FROM dbo.pdo_type_normalization';

    private PDO $pdo;

    public function setUp(): void
    {
        $this->pdo = PdoTestConnection::createConnection();
        $this->pdo->exec(
            "IF OBJECT_ID('dbo.pdo_type_normalization', 'U') IS NOT NULL DROP TABLE dbo.pdo_type_normalization",
        );
        $this->pdo->exec('CREATE TABLE dbo.pdo_type_normalization (
            [id] int IDENTITY(1,1) NOT NULL,
            [big] bigint NULL,
            [small] smallint NULL,
            [tiny] tinyint NULL,
            [price] decimal(18,2) NULL,
            [amount] money NULL,
            [ratio] float NULL,
            [name] nvarchar(255) NULL,
            [note] nvarchar(max) NULL,
            [data] varbinary(max) NULL,
            [day] date NULL,
            [created] datetime2 NULL,
            [changed] datetime NULL,
            [offset] datetimeoffset NULL,
            [t] time NULL,
            [flag] bit NULL
        )');
    }

    public function testColumnTypesAreReadFromTheDeclaredType(): void
    {
        $expected = [
            'id' => 'int',
            'big' => 'bigint',
            'small' => 'smallint',
            'tiny' => 'tinyint',
            'price' => 'decimal',
            'amount' => 'money',
            'ratio' => 'float',
            'name' => 'nvarchar',
            'note' => 'nvarchar',
            'data' => 'varbinary',
            'day' => 'date',
            'created' => 'datetime2',
            'changed' => 'datetime',
            'offset' => 'datetimeoffset',
            't' => 'time',
            'flag' => 'bit',
        ];

        $actual = [];
        foreach ($this->createMetadata()->getColumns() as $column) {
            $actual[$column->getName()] = $column->getType();
        }

        self::assertSame($expected, $actual);
    }

    private function createMetadata(): MssqlPdoQueryMetadata
    {
        /** @var PDOStatement $stmt */
        $stmt = $this->pdo->prepare(self::QUERY);
        $stmt->execute();

        return new MssqlPdoQueryMetadata($stmt);
    }
}
