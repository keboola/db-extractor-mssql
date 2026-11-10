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
            [id] int NOT NULL,
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

    public function testDeclaredTypesAreReadFromTheDriver(): void
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
        foreach ($this->createMetadata()->getDeclaredTypeColumns() as $column) {
            $actual[$column->getName()] = $column->getType();
        }

        self::assertSame($expected, $actual);
    }

    /**
     * @dataProvider identityColumnProvider
     */
    public function testIdentityColumnIsReportedAsItsBaseType(string $columnType, string $expectedType): void
    {
        $this->pdo->exec('DROP TABLE IF EXISTS dbo.pdo_identity_normalization');
        $this->pdo->exec(sprintf(
            'CREATE TABLE dbo.pdo_identity_normalization ([id] %s IDENTITY(1,1) NOT NULL, [name] nvarchar(255) NULL)',
            $columnType,
        ));

        $actual = [];
        $metadata = $this->createMetadata('SELECT [id], [name] FROM dbo.pdo_identity_normalization');
        foreach ($metadata->getDeclaredTypeColumns() as $column) {
            $actual[$column->getName()] = $column->getType();
        }

        self::assertSame(['id' => $expectedType, 'name' => 'nvarchar'], $actual);
    }

    public function identityColumnProvider(): array
    {
        return [
            'int' => ['int', 'int'],
            'bigint' => ['bigint', 'bigint'],
            'smallint' => ['smallint', 'smallint'],
            'tinyint' => ['tinyint', 'tinyint'],
            'decimal' => ['decimal(18,0)', 'decimal'],
            'numeric' => ['numeric(10,0)', 'numeric'],
        ];
    }

    /**
     * Guards the backwards compatibility of every existing configuration: getColumns() must keep
     * returning the "native_type" value, which pdo_sqlsrv reports as "string" for every type.
     */
    public function testGetColumnsKeepsTheDriverNativeType(): void
    {
        $types = [];
        foreach ($this->createMetadata()->getColumns() as $column) {
            $types[] = $column->getType();
        }

        self::assertNotEmpty($types);
        self::assertSame(['string'], array_values(array_unique($types)));
    }

    private function createMetadata(string $query = self::QUERY): MssqlPdoQueryMetadata
    {
        /** @var PDOStatement $stmt */
        $stmt = $this->pdo->prepare($query);
        $stmt->execute();

        return new MssqlPdoQueryMetadata($stmt);
    }
}
