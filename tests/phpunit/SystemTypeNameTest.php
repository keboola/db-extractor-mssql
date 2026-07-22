<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Metadata\SystemTypeName;
use PHPUnit\Framework\TestCase;

class SystemTypeNameTest extends TestCase
{
    /**
     * @dataProvider systemTypeNameProvider
     */
    public function testParse(string $systemTypeName, string $expectedType, ?string $expectedLength): void
    {
        $parsed = SystemTypeName::parse($systemTypeName);
        $this->assertSame($expectedType, $parsed->getType());
        $this->assertSame($expectedLength, $parsed->getLength());
    }

    public function systemTypeNameProvider(): array
    {
        return [
            'int' => ['int', 'int', null],
            'bigint' => ['bigint', 'bigint', null],
            'bit' => ['bit', 'bit', null],
            'float' => ['float', 'float', null],
            'real' => ['real', 'real', null],
            'money' => ['money', 'money', null],
            'xml' => ['xml', 'xml', null],
            'uniqueidentifier' => ['uniqueidentifier', 'uniqueidentifier', null],
            'date' => ['date', 'date', null],
            'datetime' => ['datetime', 'datetime', null],
            'smalldatetime' => ['smalldatetime', 'smalldatetime', null],
            'decimal' => ['decimal(18,2)', 'decimal', '18,2'],
            'decimal with space' => ['decimal(18, 2)', 'decimal', '18,2'],
            'numeric' => ['numeric(18,0)', 'numeric', '18,0'],
            'varchar' => ['varchar(8000)', 'varchar', '8000'],
            'nvarchar' => ['nvarchar(255)', 'nvarchar', '255'],
            'nvarchar max' => ['nvarchar(max)', 'nvarchar', null],
            'varbinary' => ['varbinary(50)', 'varbinary', '50'],
            'varbinary max' => ['varbinary(max)', 'varbinary', null],
            'char' => ['char(10)', 'char', '10'],
            'datetime2' => ['datetime2(7)', 'datetime2', null],
            'datetime2 low precision' => ['datetime2(0)', 'datetime2', null],
            'datetimeoffset' => ['datetimeoffset(7)', 'datetimeoffset', null],
            'time' => ['time(7)', 'time', null],
            'uppercase input is lowercased' => ['DATETIME2(7)', 'datetime2', null],
            'surrounding whitespace' => [' datetime2(7) ', 'datetime2', null],
            'empty string' => ['', '', null],
        ];
    }
}
