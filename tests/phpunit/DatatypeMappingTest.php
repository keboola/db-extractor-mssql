<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\MssqlDataType;
use PHPUnit\Framework\TestCase;

class DatatypeMappingTest extends TestCase
{
    /**
     * @dataProvider columnMetadataProvider
     * @param array $testColumn
     * @param array $expectedMetadtata
     */
    public function testDatatypeMapping(array $testColumn, array $expectedMetadtata): void
    {
        $datatype = new MssqlDataType(
            $testColumn['type'],
            array_intersect_key($testColumn, array_flip(MssqlDataType::DATATYPE_KEYS))
        );
        $datatypeMetadata = $datatype->toMetadata();
        $this->assertEquals($expectedMetadtata, $datatypeMetadata);
    }

    public function columnMetadataProvider(): array
    {
        return [
            // integer column
            [
                [
                    'name' => 'some int',
                    'sanitizedName' => 'some_int',
                    'type' => 'int',
                    'length' => 10,
                    'nullable' => false,
                    'ordinalPosition' => 1,
                    'primaryKey' => true,
                    'primaryKeyName' => 'PK_AUTOINC',
                ],
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'int',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => 10,
                    ],
                ],
            ],
            // timestamp column
            [
                [
                    'name' => 'some timestamp',
                    'sanitizedName' => 'some_timestamp',
                    'type' => 'timestamp',
                    'length' => null,
                    'nullable' => false,
                    'ordinalPosition' => 1,
                ],
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'timestamp',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                ],
            ],
            // bit column
            [
                [
                    'name' => 'some bit',
                    'sanitizedName' => 'some_bit',
                    'type' => 'bit',
                    'length' => null,
                    'nullable' => false,
                    'ordinalPosition' => 1,
                ],
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'bit',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'BOOLEAN',
                    ],
                ],
            ],
            // xml column
            [
                [
                    'name' => 'some xml',
                    'sanitizedName' => 'some_xml',
                    'type' => 'xml',
                    'length' => null,
                    'nullable' => false,
                    'ordinalPosition' => 1,
                ],
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'xml',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                ],
            ],
        ];
    }
}
