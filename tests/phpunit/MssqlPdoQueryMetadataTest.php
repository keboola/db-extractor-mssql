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
            [exact] numeric(18,6) NULL,
            [amount] money NULL,
            [smallamount] smallmoney NULL,
            [ratio] float NULL,
            [approx] real NULL,
            [code] char(10) NULL,
            [label] varchar(100) NULL,
            [name] nvarchar(255) NULL,
            [note] nvarchar(max) NULL,
            [oldtext] text NULL,
            [oldntext] ntext NULL,
            [guid] uniqueidentifier NULL,
            [doc] xml NULL,
            [fixedbin] binary(8) NULL,
            [data] varbinary(max) NULL,
            [day] date NULL,
            [created] datetime2 NULL,
            [changed] datetime NULL,
            [rounded] smalldatetime NULL,
            [offset] datetimeoffset NULL,
            [t] time NULL,
            [flag] bit NULL,
            [version] rowversion NOT NULL
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
            'exact' => 'numeric',
            'amount' => 'money',
            'smallamount' => 'smallmoney',
            'ratio' => 'float',
            'approx' => 'real',
            'code' => 'char',
            'label' => 'varchar',
            'name' => 'nvarchar',
            'note' => 'nvarchar',
            'oldtext' => 'text',
            'oldntext' => 'ntext',
            'guid' => 'uniqueidentifier',
            'doc' => 'xml',
            'fixedbin' => 'binary',
            'data' => 'varbinary',
            'day' => 'date',
            'created' => 'datetime2',
            'changed' => 'datetime',
            'rounded' => 'smalldatetime',
            'offset' => 'datetimeoffset',
            't' => 'time',
            'flag' => 'bit',
            // The driver names a rowversion column by its old synonym.
            'version' => 'timestamp',
        ];

        self::assertSame($expected, $this->getDeclaredTypes(self::QUERY));
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

        self::assertSame(
            ['id' => $expectedType, 'name' => 'nvarchar'],
            $this->getDeclaredTypes('SELECT [id], [name] FROM dbo.pdo_identity_normalization'),
        );
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
     * The BCP path reports an alias type (CREATE TYPE ... FROM) by its system type. The PDO path
     * must report the same name, otherwise the column would be STRING on one path only.
     */
    public function testAliasTypeIsReportedAsItsBaseType(): void
    {
        $aliasTypes = [
            'pdo_alias_int' => 'int',
            'pdo_alias_decimal' => 'decimal(18,2)',
            'pdo_alias_datetime' => 'datetime2',
        ];

        $this->dropAliasTypeFixtures(array_keys($aliasTypes));
        foreach ($aliasTypes as $aliasType => $systemType) {
            $this->pdo->exec(sprintf('CREATE TYPE dbo.%s FROM %s NULL', $aliasType, $systemType));
        }
        $this->pdo->exec('CREATE TABLE dbo.pdo_alias_normalization (
            [quantity] dbo.pdo_alias_int NULL,
            [price] dbo.pdo_alias_decimal NULL,
            [created] dbo.pdo_alias_datetime NULL
        )');

        try {
            $actual = $this->getDeclaredTypes('SELECT * FROM dbo.pdo_alias_normalization');
        } finally {
            $this->dropAliasTypeFixtures(array_keys($aliasTypes));
        }

        self::assertSame(['quantity' => 'int', 'price' => 'decimal', 'created' => 'datetime2'], $actual);
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

    private function getDeclaredTypes(string $query): array
    {
        /** @var PDOStatement $stmt */
        $stmt = $this->pdo->prepare($query);
        $stmt->execute();

        $types = [];
        foreach ((new MssqlPdoQueryMetadata($stmt))->getDeclaredTypeColumns() as $column) {
            $types[$column->getName()] = $column->getType();
        }
        // An open result set keeps a lock on the table, so a later DROP TABLE would wait for it.
        $stmt->closeCursor();

        return $types;
    }

    private function dropAliasTypeFixtures(array $aliasTypes): void
    {
        // The table goes first: a type cannot be dropped while a column uses it.
        $this->pdo->exec('DROP TABLE IF EXISTS dbo.pdo_alias_normalization');
        foreach ($aliasTypes as $aliasType) {
            $this->pdo->exec(sprintf('DROP TYPE IF EXISTS dbo.%s', $aliasType));
        }
    }

    private function createMetadata(): MssqlPdoQueryMetadata
    {
        /** @var PDOStatement $stmt */
        $stmt = $this->pdo->prepare(self::QUERY);
        $stmt->execute();

        return new MssqlPdoQueryMetadata($stmt);
    }
}
