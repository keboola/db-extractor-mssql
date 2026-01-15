<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Configuration\MssqlDatabaseConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;
use PHPUnit\Framework\TestCase;

class MssqlDatabaseConfigTest extends TestCase
{
    public function testFromArrayWithApplicationIntent(): void
    {
        $data = [
            'host' => 'localhost',
            'user' => 'testuser',
            '#password' => 'testpass',
            'database' => 'testdb',
            'applicationIntent' => 'ReadOnly',
        ];

        $config = MssqlDatabaseConfig::fromArray($data);

        $this->assertTrue($config->hasApplicationIntent());
        $this->assertEquals('ReadOnly', $config->getApplicationIntent());
    }

    public function testFromArrayWithoutApplicationIntent(): void
    {
        $data = [
            'host' => 'localhost',
            'user' => 'testuser',
            '#password' => 'testpass',
            'database' => 'testdb',
        ];

        $config = MssqlDatabaseConfig::fromArray($data);

        $this->assertFalse($config->hasApplicationIntent());
    }

    public function testGetApplicationIntentThrowsExceptionWhenNotSet(): void
    {
        $data = [
            'host' => 'localhost',
            'user' => 'testuser',
            '#password' => 'testpass',
            'database' => 'testdb',
        ];

        $config = MssqlDatabaseConfig::fromArray($data);

        $this->expectException(PropertyNotSetException::class);
        $this->expectExceptionMessage('ApplicationIntent is not set.');
        $config->getApplicationIntent();
    }

    public function testFromArrayWithReadWriteApplicationIntent(): void
    {
        $data = [
            'host' => 'localhost',
            'user' => 'testuser',
            '#password' => 'testpass',
            'database' => 'testdb',
            'applicationIntent' => 'ReadWrite',
        ];

        $config = MssqlDatabaseConfig::fromArray($data);

        $this->assertTrue($config->hasApplicationIntent());
        $this->assertEquals('ReadWrite', $config->getApplicationIntent());
    }
}
