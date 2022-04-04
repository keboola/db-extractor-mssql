<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Keboola\DbExtractor\FunctionalTests\DatabaseManager;

/**
 * @return string[]
 */
function getColumns(): array
{
    return [
        'col1' => 'text',
        'col2' => 'text',
        'col3' => 'text',
        'col4' => 'text',
        'col5' => 'text',
        'col6' => 'text',
        'col7' => 'text',
        'col8' => 'text',
        'col9' => 'text',
        'col10' => 'text',
        'col11' => 'text',
        'col12' => 'text',
        'col13' => 'text',
        'col14' => 'text',
        'col15' => 'text',
        'col16' => 'text',
        'col17' => 'text',
        'col18' => 'text',
        'col19' => 'text',
        'col20' => 'text',
        'col21' => 'text',
        'col22' => 'text',
        'col23' => 'text',
        'col24' => 'text',
        'col25' => 'text',
        'col26' => 'text',
        'col27' => 'text',
        'col28' => 'text',
        'col29' => 'text',
        'col30' => 'text',
    ];
}

/**
 * @return string[][]
 */
function getRows(): array
{
    return [[
        '0000021270',
        '99999.000000000000',
        '1.000000000000',
        '67',
        'P067T03',
        'LSR',
        '0',
        '10.000000000000',
        '.000000000000',
        '25.900000000000',
        '3',
        'N0078',
        '`trboholy zamstnanciOC",1,9205502,,",0 0000021271"',
        '1.000000000000',
        '1.000000000000',
        '21',
        'P021T02',
        'LSR',
        '3',
        '30.000000000000',
        '.000000000000',
        '.000000000000',
        '0',
        '',
        '',
        '1',
        '8953046',
        '',
        '',
        '0',
    ]];
}

return static function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());
    $manager->createTable('escaping-issue', getColumns());
    $manager->insertRows('escaping-issue', array_keys(getColumns()), getRows());
};
