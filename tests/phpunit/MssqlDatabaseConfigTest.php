<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Configuration\MssqlDatabaseConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;
use PHPUnit\Framework\TestCase;

class MssqlDatabaseConfigTest extends TestCase
{
    public function testSqlLoginConfig(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'localhost',
            'database' => 'test',
            'user' => 'sa',
            '#password' => 'secret',
        ]);

        $this->assertFalse($config->hasServicePrincipal());
        $this->assertSame('sa', $config->getConnectionUsername());
        $this->assertSame('secret', $config->getConnectionPassword());
    }

    public function testServicePrincipalDetected(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'server.database.windows.net',
            'database' => 'test',
            'tenantId' => 'tenant-id',
            'clientId' => 'client-id',
            '#clientSecret' => 'client-secret',
        ]);

        $this->assertTrue($config->hasServicePrincipal());
        $this->assertSame('tenant-id', $config->getTenantId());
        $this->assertSame('client-id', $config->getClientId());
        $this->assertSame('client-secret', $config->getClientSecret());
        // For SP the ODBC UID/PWD are the client id/secret.
        $this->assertSame('client-id', $config->getConnectionUsername());
        $this->assertSame('client-secret', $config->getConnectionPassword());
    }

    public function testPartialServicePrincipalIsNotDetected(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'localhost',
            'database' => 'test',
            'user' => 'sa',
            '#password' => 'secret',
            'clientId' => 'client-id',
        ]);

        $this->assertFalse($config->hasServicePrincipal());
        $this->assertSame('sa', $config->getConnectionUsername());
    }

    public function testGettersThrowWhenServicePrincipalNotSet(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'localhost',
            'database' => 'test',
            'user' => 'sa',
            '#password' => 'secret',
        ]);

        $this->expectException(PropertyNotSetException::class);
        $config->getTenantId();
    }
}
