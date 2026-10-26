<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Metadata\MssqlMetadataProvider;
use Keboola\DbExtractor\Tests\Stubs\CapturingMssqlPdoConnection;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class MssqlMetadataProviderTest extends TestCase
{
    public function testDescriptionsArePropagated(): void
    {
        $connection = $this->createConnection([
            $this->descriptionRow(null, 'Registered users'),
            $this->descriptionRow('id', 'Surrogate key'),
        ]);

        $table = (new MssqlMetadataProvider($connection, true))
            ->listTables()
            ->getByNameAndSchema('users', 'dbo');

        Assert::assertTrue($table->hasDescription());
        Assert::assertSame('Registered users', $table->getDescription());

        $columns = $table->getColumns();
        Assert::assertSame('Surrogate key', $columns->getByName('id')->getDescription());
        // A column with no MS_Description must not get a description
        Assert::assertFalse($columns->getByName('email')->hasDescription());
    }

    public function testDescriptionsAreNotPropagatedWhenDisabled(): void
    {
        $connection = $this->createConnection([
            $this->descriptionRow(null, 'Registered users'),
            $this->descriptionRow('id', 'Surrogate key'),
        ]);

        $table = (new MssqlMetadataProvider($connection, false))
            ->listTables()
            ->getByNameAndSchema('users', 'dbo');

        Assert::assertFalse($table->hasDescription());
        Assert::assertFalse($table->getColumns()->getByName('id')->hasDescription());

        // The extended properties are not even read
        foreach ($connection->getQueries() as $query) {
            Assert::assertStringNotContainsString('MS_Description', $query);
        }
    }

    public function testEmptyDescriptionIsTreatedAsNoDescription(): void
    {
        // SQL Server does store an empty extended property, unlike Postgres, which discards it
        $connection = $this->createConnection([
            $this->descriptionRow(null, ''),
            $this->descriptionRow('id', '   '),
        ]);

        $table = (new MssqlMetadataProvider($connection, true))
            ->listTables()
            ->getByNameAndSchema('users', 'dbo');

        Assert::assertFalse($table->hasDescription());
        Assert::assertFalse($table->getColumns()->getByName('id')->hasDescription());
    }

    /**
     * A column repeated by the constraint joins of getColumnsSqlComplex() must keep
     * its description rather than tripping over the reused column builder.
     */
    public function testDescriptionSurvivesADuplicatedColumnRow(): void
    {
        $connection = new CapturingMssqlPdoConnection(
            [$this->descriptionRow('id', 'Surrogate key')],
            $this->tableRows(),
            [
                $this->columnRow('id', 1, 'int') + ['pk_name' => 'PK_users'],
                $this->columnRow('id', 1, 'int') + ['uk_name' => 'UK_users'],
            ],
        );

        $table = (new MssqlMetadataProvider($connection, true))
            ->listTables([], true)
            ->getByNameAndSchema('users', 'dbo');

        $columns = $table->getColumns();
        Assert::assertCount(1, $columns->getAll());
        Assert::assertSame('Surrogate key', $columns->getByName('id')->getDescription());
    }

    public function testDescriptionsQueryReadsColumnsWhenColumnsAreLoaded(): void
    {
        $connection = $this->createConnection([]);
        (new MssqlMetadataProvider($connection, true))->listTables();

        $sql = $connection->getDescriptionsQuery();
        Assert::assertStringContainsString('[c].[name] AS [COLUMN_NAME]', $sql);
        Assert::assertStringContainsString('LEFT JOIN [sys].[columns] AS [c]', $sql);
        Assert::assertStringContainsString('([ep].[minor_id] = 0 OR [c].[name] IS NOT NULL)', $sql);
    }

    public function testDescriptionsQueryReadsTheTableDescriptionOnlyWithoutColumns(): void
    {
        $connection = $this->createConnection([]);
        (new MssqlMetadataProvider($connection, true))->listTables([], false);

        $sql = $connection->getDescriptionsQuery();
        Assert::assertStringContainsString('[ep].[minor_id] = 0', $sql);
        Assert::assertStringNotContainsString('COLUMN_NAME', $sql);
        Assert::assertStringNotContainsString('[sys].[columns]', $sql);
    }

    /**
     * The whole point of reading the descriptions with a query of their own: the tables and
     * columns queries must come out identical whatever the toggle is set to.
     *
     * @dataProvider loadColumnsProvider
     */
    public function testTheExistingQueriesAreUntouched(bool $loadColumns): void
    {
        $enabled = $this->createConnection([]);
        (new MssqlMetadataProvider($enabled, true))->listTables([], $loadColumns);

        $disabled = $this->createConnection([]);
        (new MssqlMetadataProvider($disabled, false))->listTables([], $loadColumns);

        $withoutDescriptions = array_values(array_filter(
            $enabled->getQueries(),
            fn (string $query): bool => !str_contains($query, 'MS_Description'),
        ));

        Assert::assertSame($disabled->getQueries(), $withoutDescriptions);
    }

    public function loadColumnsProvider(): array
    {
        return [
            'with columns' => [true],
            'without columns' => [false],
        ];
    }

    /**
     * @param array<array<string, mixed>> $descriptions
     */
    private function createConnection(array $descriptions): CapturingMssqlPdoConnection
    {
        return new CapturingMssqlPdoConnection(
            $descriptions,
            $this->tableRows(),
            [
                $this->columnRow('id', 1, 'int'),
                $this->columnRow('email', 2, 'varchar'),
            ],
        );
    }

    /**
     * @return array<array<string, mixed>>
     */
    private function tableRows(): array
    {
        return [
            [
                'TABLE_CATALOG' => 'test',
                'TABLE_SCHEMA' => 'dbo',
                'TABLE_NAME' => 'users',
                'TABLE_TYPE' => 'BASE TABLE',
                'is_tracked_by_cdc' => '0',
            ],
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function descriptionRow(?string $columnName, string $description): array
    {
        return [
            'TABLE_SCHEMA' => 'dbo',
            'TABLE_NAME' => 'users',
            'COLUMN_NAME' => $columnName,
            'DESCRIPTION' => $description,
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function columnRow(string $name, int $ordinalPosition, string $dataType): array
    {
        return [
            'TABLE_SCHEMA' => 'dbo',
            'TABLE_NAME' => 'users',
            'COLUMN_NAME' => $name,
            'ORDINAL_POSITION' => (string) $ordinalPosition,
            'DATA_TYPE' => $dataType,
            'IS_NULLABLE' => 'YES',
            'NUMERIC_PRECISION' => $dataType === 'int' ? '10' : '0',
            'NUMERIC_SCALE' => '0',
            'CHARACTER_MAXIMUM_LENGTH' => $dataType === 'varchar' ? '100' : null,
        ];
    }
}
